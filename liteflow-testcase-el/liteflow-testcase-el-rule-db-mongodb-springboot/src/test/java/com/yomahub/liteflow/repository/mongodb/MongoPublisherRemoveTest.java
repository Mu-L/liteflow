package com.yomahub.liteflow.repository.mongodb;

import com.mongodb.MongoWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteError;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.TransactionBody;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.PublishResult;
import com.yomahub.liteflow.publisher.PublishScriptRequest;
import com.yomahub.liteflow.publisher.RemoveRuleRequest;
import com.yomahub.liteflow.publisher.RulePublisher;
import com.yomahub.liteflow.publisher.exception.RuleStorageException;
import com.yomahub.liteflow.publisher.exception.VersionConflictException;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Remove-path and compare-and-swap contract tests driven by driver mocks. */
@SuppressWarnings({ "unchecked", "rawtypes" })
class MongoPublisherRemoveTest {

	private MongoClient client;
	private MongoDatabase database;
	private MongoCollection<Document> chains;
	private MongoCollection<Document> scripts;
	private MongoCollection<Document> changes;
	private MongoCollection<Document> sequences;

	@BeforeEach
	void setUp() {
		client = mock(MongoClient.class);
		database = mock(MongoDatabase.class);
		chains = mock(MongoCollection.class);
		scripts = mock(MongoCollection.class);
		changes = mock(MongoCollection.class);
		sequences = mock(MongoCollection.class);
		when(client.getDatabase("rules")).thenReturn(database);
		when(database.getCollection("lf_chain")).thenReturn(chains);
		when(database.getCollection("lf_script")).thenReturn(scripts);
		when(database.getCollection("lf_change_log")).thenReturn(changes);
		when(database.getCollection("lf_sequence")).thenReturn(sequences);
	}

	@Test
	void removeExistingChainDeletesWithCasAndWritesDeleteChangeLog() {
		transactionSession();
		stubFind(chains, new Document("version", 3L));
		when(chains.deleteOne(any(ClientSession.class), any(Bson.class))).thenReturn(DeleteResult.acknowledged(1));
		when(sequences.findOneAndUpdate(any(ClientSession.class), any(Bson.class), any(Bson.class), any()))
				.thenReturn(new Document("seq", 7L));

		PublishResult result;
		try (RulePublisher publisher = new MongoRulePublisherProvider().create(config())) {
			result = publisher.removeChain(RemoveRuleRequest.builder().targetId("c1").expectedVersion(3L).build());
		}

		assertEquals(ChangeRecord.Op.DELETE, result.getOperation());
		assertEquals(ChangeRecord.TargetType.CHAIN, result.getTargetType());
		assertEquals(3, result.getVersion());
		assertEquals(7, result.getSequence());
		verify(chains).deleteOne(any(ClientSession.class), any(Bson.class));
		verify(changes).insertOne(any(ClientSession.class), any(Document.class));
	}

	@Test
	void removeExistingScriptDeletesAndWritesDeleteChangeLog() {
		transactionSession();
		stubFind(scripts, new Document("version", 1L));
		when(scripts.deleteOne(any(ClientSession.class), any(Bson.class))).thenReturn(DeleteResult.acknowledged(1));
		when(sequences.findOneAndUpdate(any(ClientSession.class), any(Bson.class), any(Bson.class), any()))
				.thenReturn(new Document("seq", 2L));

		PublishResult result;
		try (RulePublisher publisher = new MongoRulePublisherProvider().create(config())) {
			result = publisher.removeScript(RemoveRuleRequest.builder().targetId("s1").build());
		}

		assertEquals(ChangeRecord.Op.DELETE, result.getOperation());
		assertEquals(ChangeRecord.TargetType.SCRIPT, result.getTargetType());
		verify(scripts).deleteOne(any(ClientSession.class), any(Bson.class));
	}

	@Test
	void removeWithStaleExpectedVersionConflictsBeforeDelete() {
		transactionSession();
		stubFind(chains, new Document("version", 3L));

		try (RulePublisher publisher = new MongoRulePublisherProvider().create(config())) {
			assertThrows(VersionConflictException.class, () -> publisher
					.removeChain(RemoveRuleRequest.builder().targetId("c1").expectedVersion(2L).build()));
		}

		verify(chains, never()).deleteOne(any(ClientSession.class), any(Bson.class));
		verify(changes, never()).insertOne(any(ClientSession.class), any(Document.class));
	}

	@Test
	void removeWithExpectedZeroAlwaysConflicts() {
		transactionSession();
		stubFind(chains, null);

		try (RulePublisher publisher = new MongoRulePublisherProvider().create(config())) {
			assertThrows(VersionConflictException.class, () -> publisher
					.removeChain(RemoveRuleRequest.builder().targetId("ghost").expectedVersion(0L).build()));
		}

		verify(chains, never()).deleteOne(any(ClientSession.class), any(Bson.class));
	}

	@Test
	void removeMissingTargetWithoutExpectedStillAdvancesSequence() {
		transactionSession();
		stubFind(chains, null);
		when(sequences.findOneAndUpdate(any(ClientSession.class), any(Bson.class), any(Bson.class), any()))
				.thenReturn(new Document("seq", 5L));

		PublishResult result;
		try (RulePublisher publisher = new MongoRulePublisherProvider().create(config())) {
			result = publisher.removeChain(RemoveRuleRequest.builder().targetId("ghost").build());
		}

		assertEquals(ChangeRecord.Op.DELETE, result.getOperation());
		assertEquals(0, result.getVersion());
		assertEquals(5, result.getSequence());
		verify(chains, never()).deleteOne(any(ClientSession.class), any(Bson.class));
		verify(changes).insertOne(any(ClientSession.class), any(Document.class));
	}

	@Test
	void removeDeleteCasLossRetriesUntilLimit() {
		transactionSession();
		stubFind(chains, new Document("version", 2L));
		when(chains.deleteOne(any(ClientSession.class), any(Bson.class))).thenReturn(DeleteResult.acknowledged(0));

		try (RulePublisher publisher = new MongoRulePublisherProvider().create(config())) {
			RuleStorageException error = assertThrows(RuleStorageException.class,
					() -> publisher.removeChain(RemoveRuleRequest.builder().targetId("c1").build()));
			assertTrue(error.getMessage().contains("exceeded concurrent retry limit"));
		}

		verify(chains, times(8)).deleteOne(any(ClientSession.class), any(Bson.class));
	}

	@Test
	void removeDeleteCasLossWithExpectedConflictsImmediately() {
		transactionSession();
		stubFind(chains, new Document("version", 2L));
		when(chains.deleteOne(any(ClientSession.class), any(Bson.class))).thenReturn(DeleteResult.acknowledged(0));

		try (RulePublisher publisher = new MongoRulePublisherProvider().create(config())) {
			assertThrows(VersionConflictException.class, () -> publisher
					.removeChain(RemoveRuleRequest.builder().targetId("c1").expectedVersion(2L).build()));
		}

		verify(chains, times(1)).deleteOne(any(ClientSession.class), any(Bson.class));
	}

	@Test
	void publishDuplicateKeyWithoutExpectedRetriesUntilLimit() {
		transactionSession();
		stubFind(chains, null);
		when(chains.insertOne(any(ClientSession.class), any(Document.class)))
				.thenThrow(writeError(11000, "duplicate key"));

		try (RulePublisher publisher = new MongoRulePublisherProvider().create(config())) {
			RuleStorageException error = assertThrows(RuleStorageException.class,
					() -> publisher.publishChain(chain("c1", null)));
			assertTrue(error.getMessage().contains("exceeded concurrent retry limit"));
		}

		verify(chains, times(8)).insertOne(any(ClientSession.class), any(Document.class));
	}

	@Test
	void publishDuplicateKeyWithExpectedConflictsImmediately() {
		transactionSession();
		stubFind(chains, null);
		when(chains.insertOne(any(ClientSession.class), any(Document.class)))
				.thenThrow(writeError(11000, "duplicate key"));

		try (RulePublisher publisher = new MongoRulePublisherProvider().create(config())) {
			assertThrows(VersionConflictException.class,
					() -> publisher.publishChain(chain("c1", 0L)));
		}

		verify(chains, times(1)).insertOne(any(ClientSession.class), any(Document.class));
	}

	@Test
	void publishNonDuplicateWriteErrorIsWrappedWithoutReplicaHint() {
		transactionSession();
		stubFind(chains, null);
		when(chains.insertOne(any(ClientSession.class), any(Document.class)))
				.thenThrow(writeError(13, "not authorized"));

		try (RulePublisher publisher = new MongoRulePublisherProvider().create(config())) {
			RuleStorageException error = assertThrows(RuleStorageException.class,
					() -> publisher.publishChain(chain("c1", null)));
			assertTrue(error.getMessage().contains("not authorized"));
			assertTrue(!error.getMessage().contains("replica set"));
		}
	}

	@Test
	void publishReplaceCasLossWithExpectedConflictsImmediately() {
		transactionSession();
		stubFind(chains, new Document("version", 2L));
		when(chains.replaceOne(any(ClientSession.class), any(Bson.class), any(Document.class)))
				.thenReturn(UpdateResult.acknowledged(0, 0L, null));

		try (RulePublisher publisher = new MongoRulePublisherProvider().create(config())) {
			assertThrows(VersionConflictException.class,
					() -> publisher.publishChain(chain("c1", 2L)));
		}

		verify(chains, times(1)).replaceOne(any(ClientSession.class), any(Bson.class), any(Document.class));
	}

	@Test
	void publishScriptCreatesDocumentAndChangeLog() {
		transactionSession();
		stubFind(scripts, null);
		when(sequences.findOneAndUpdate(any(ClientSession.class), any(Bson.class), any(Bson.class), any()))
				.thenReturn(new Document("seq", 3L));

		PublishResult result;
		try (RulePublisher publisher = new MongoRulePublisherProvider().create(config())) {
			result = publisher.publishScript(PublishScriptRequest.builder().nodeId("s1")
					.script("return 1").name("s1").type("script").language("groovy").build());
		}

		assertEquals(1, result.getVersion());
		assertEquals(3, result.getSequence());
		assertEquals(ChangeRecord.TargetType.SCRIPT, result.getTargetType());
		verify(scripts).insertOne(any(ClientSession.class), any(Document.class));
		verify(changes).insertOne(any(ClientSession.class), any(Document.class));
	}

	private MongoPublisherConfig config() {
		return MongoPublisherConfig.builder().applicationName("app").database("rules")
				.collectionPrefix("lf_").mongoClient(client).build();
	}

	private ClientSession transactionSession() {
		ClientSession session = mock(ClientSession.class);
		when(client.startSession()).thenReturn(session);
		when(session.withTransaction(any(TransactionBody.class), any())).thenAnswer(invocation -> {
			TransactionBody<?> body = invocation.getArgument(0);
			return body.execute();
		});
		return session;
	}

	private void stubFind(MongoCollection<Document> collection, Document first) {
		FindIterable<Document> iterable = mock(FindIterable.class);
		when(iterable.projection(any(Bson.class))).thenReturn(iterable);
		when(iterable.first()).thenReturn(first);
		when(collection.find(any(ClientSession.class), any(Bson.class))).thenReturn(iterable);
	}

	private MongoWriteException writeError(int code, String message) {
		return new MongoWriteException(new WriteError(code, message, new BsonDocument()), new ServerAddress());
	}

	private PublishChainRequest chain(String id, Long expected) {
		return PublishChainRequest.builder().chainId(id).el("THEN(a)").expectedVersion(expected).build();
	}
}
