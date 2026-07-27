package com.yomahub.liteflow.repository.mongodb;

import com.mongodb.MongoException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.TransactionBody;
import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.exception.SeqGapException;
import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.PublishResult;
import com.yomahub.liteflow.publisher.RulePublisher;
import com.yomahub.liteflow.publisher.exception.PublisherConfigurationException;
import com.yomahub.liteflow.publisher.exception.RuleStorageException;
import com.yomahub.liteflow.publisher.exception.RuleValidationException;
import com.yomahub.liteflow.publisher.exception.VersionConflictException;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import com.yomahub.liteflow.repository.vo.ChainRecord;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@SuppressWarnings({ "unchecked", "rawtypes" })
class MongoMockContractTest {

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
	void providerValidatesConfigurationAndBuildsAgainstBorrowedMockClient() {
		MongoRulePublisherProvider provider = new MongoRulePublisherProvider();
		MongoPublisherConfig config = config();

		assertTrue(provider.supports(config));
		try (RulePublisher ignored = provider.create(config)) {
			verify(client, never()).startSession();
		}
		verify(client, never()).close();

		assertThrows(PublisherConfigurationException.class,
				() -> provider.create(MongoPublisherConfig.builder().applicationName("app").build()));
		assertThrows(PublisherConfigurationException.class,
				() -> provider.create(MongoPublisherConfig.builder().applicationName("app")
						.uri("mongodb://mock.invalid").database("bad.name").build()));
		assertThrows(PublisherConfigurationException.class,
				() -> provider.create(MongoPublisherConfig.builder().applicationName("app")
						.uri("mongodb://mock.invalid").collectionPrefix("lf-$").build()));
	}

	@Test
	void publisherCreatesChainAndSequenceInsideMockTransaction() {
		ClientSession session = transactionSession();
		FindIterable<Document> current = iterable(null);
		when(chains.find(any(ClientSession.class), any(Bson.class))).thenReturn(current);
		when(sequences.findOneAndUpdate(any(ClientSession.class), any(Bson.class), any(Bson.class), any()))
				.thenReturn(new Document("seq", 4L));

		PublishResult result;
		try (RulePublisher publisher = new MongoRulePublisherProvider().create(config())) {
			result = publisher.publishChain(chain("c1", "THEN(a)", 0L));
		}

		assertEquals(1, result.getVersion());
		assertEquals(4, result.getSequence());
		assertEquals(ChangeRecord.Op.UPSERT, result.getOperation());
		verify(chains).insertOne(any(ClientSession.class), any(Document.class));
		verify(changes).insertOne(any(ClientSession.class), any(Document.class));
		verify(session).close();
	}

	@Test
	void staleVersionStopsBeforeAnyWrite() {
		transactionSession();
		FindIterable<Document> current = iterable(new Document("version", 3L));
		when(chains.find(any(ClientSession.class), any(Bson.class))).thenReturn(current);

		try (RulePublisher publisher = new MongoRulePublisherProvider().create(config())) {
			assertThrows(VersionConflictException.class,
					() -> publisher.publishChain(chain("c1", "THEN(b)", 2L)));
		}

		verify(chains, never()).insertOne(any(ClientSession.class), any(Document.class));
		verify(changes, never()).insertOne(any(ClientSession.class), any(Document.class));
	}

	@Test
	void transactionFailureIsMappedWithReplicaSetHint() {
		when(client.startSession()).thenThrow(new MongoException(20, "Transaction numbers are only allowed"));

		try (RulePublisher publisher = new MongoRulePublisherProvider().create(config())) {
			RuleStorageException error = assertThrows(RuleStorageException.class,
					() -> publisher.publishChain(chain("c1", "THEN(a)", 0L)));
			assertTrue(error.getMessage().contains("replica set or sharded cluster"));
		}
	}

	@Test
	void repositoryMapsChainDocumentWithoutAConnection() {
		Document document = new Document("chainId", "c1").append("el", "THEN(a)")
				.append("route", "AND(a)").append("namespace", "ns").append("version", 2L)
				.append("contentMd5", "md5").append("enable", true);
		FindIterable<Document> result = iterable(document);
		when(chains.find(any(Bson.class))).thenReturn(result);

		ChainRecord record = repository().fetchChain("c1");

		assertEquals("c1", record.getChainId());
		assertEquals("THEN(a)", record.getEl());
		assertEquals("AND(a)", record.getRoute());
		assertEquals("ns", record.getNamespace());
		assertEquals(2, record.getVersion());
		assertTrue(record.isEnable());
	}

	@Test
	void repositoryMapsOrderedChangesAndRejectsGaps() {
		FindIterable<Document> iterable = iterable(null);
		when(changes.find(any(Bson.class))).thenReturn(iterable);
		when(iterable.sort(any(Bson.class))).thenReturn(iterable);
		when(iterable.limit(anyInt())).thenReturn(iterable);
		List<Document> documents = Arrays.asList(
				change(1, "CHAIN", "c1", "UPSERT", 1),
				change(2, "SCRIPT", "s1", "DELETE", 3));
		when(iterable.into(any(List.class))).thenAnswer(invocation -> {
			List<Document> target = invocation.getArgument(0);
			target.addAll(documents);
			return target;
		});

		List<ChangeRecord> result = repository().fetchChangesSince(0, 10);
		assertEquals(2, result.size());
		assertEquals(ChangeRecord.TargetType.CHAIN, result.get(0).getTargetType());
		assertEquals(ChangeRecord.Op.DELETE, result.get(1).getOp());

		when(iterable.into(any(List.class))).thenAnswer(invocation -> {
			List<Document> target = invocation.getArgument(0);
			target.add(change(4, "CHAIN", "c4", "UPSERT", 1));
			return target;
		});
		assertThrows(SeqGapException.class, () -> repository().fetchChangesSince(2, 10));
	}

	@Test
	void validationRejectsBadBatchAndOversizedIdsBeforeDriverCalls() {
		MongoRuleRepository repository = repository();
		assertThrows(ConfigErrorException.class, () -> repository.fetchChangesSince(0, 0));
		assertThrows(ConfigErrorException.class, () -> repository.fetchChain(repeat("c", 129)));
		verify(changes, never()).find(any(Bson.class));
		verify(chains, never()).find(any(Bson.class));

		try (RulePublisher publisher = new MongoRulePublisherProvider().create(config())) {
			assertThrows(RuleValidationException.class,
					() -> publisher.publishChain(chain(repeat("c", 129), "THEN(a)", null)));
		}
		verify(client, never()).startSession();
	}

	private MongoPublisherConfig config() {
		return MongoPublisherConfig.builder().applicationName("app").database("rules")
				.collectionPrefix("lf_").mongoClient(client).build();
	}

	private MongoRuleRepository repository() {
		return new MongoRuleRepository(client, database, new MongoCollections("lf_"), "app");
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

	private FindIterable<Document> iterable(Document first) {
		FindIterable<Document> iterable = mock(FindIterable.class);
		when(iterable.projection(any(Bson.class))).thenReturn(iterable);
		when(iterable.first()).thenReturn(first);
		return iterable;
	}

	private Document change(long seq, String type, String id, String operation, long version) {
		return new Document("seq", seq).append("targetType", type).append("targetId", id)
				.append("operation", operation).append("version", version);
	}

	private PublishChainRequest chain(String id, String el, Long expected) {
		return PublishChainRequest.builder().chainId(id).el(el).expectedVersion(expected).build();
	}

	private String repeat(String value, int count) {
		StringBuilder result = new StringBuilder(value.length() * count);
		for (int i = 0; i < count; i++) { result.append(value); }
		return result.toString();
	}
}
