package com.yomahub.liteflow.repository.mongodb;

import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.TransactionBody;
import com.yomahub.liteflow.publisher.exception.RuleStorageException;
import com.yomahub.liteflow.repository.vo.ChainMeta;
import com.yomahub.liteflow.repository.vo.RuleManifest;
import com.yomahub.liteflow.repository.vo.ScriptMeta;
import com.yomahub.liteflow.repository.vo.ScriptRecord;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Read-path contract tests: manifest snapshot, meta fetches, and error mapping. */
@SuppressWarnings({ "unchecked", "rawtypes" })
class MongoRepositoryReadTest {

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
	void fetchManifestReadsSnapshotInsideTransaction() {
		ClientSession session = transactionSession();
		stubSessionForEach(chains,
				new Document("chainId", "c1").append("version", 2L).append("contentMd5", "cm"));
		stubSessionForEach(scripts, new Document("nodeId", "s1")
				.append("version", 4L).append("contentMd5", "sm").append("type", "script")
				.append("language", "groovy").append("name", "s1"));
		stubSessionFindFirst(sequences, new Document("seq", 9L));

		RuleManifest manifest = repository().fetchManifest();

		assertEquals(9, manifest.getLatestSeq());
		assertEquals(1, manifest.getChains().size());
		ChainMeta chainMeta = manifest.getChains().get(0);
		assertEquals("c1", chainMeta.getChainId());
		assertEquals(2, chainMeta.getVersion());
		assertEquals("cm", chainMeta.getMd5());
		assertEquals(1, manifest.getScripts().size());
		ScriptMeta scriptMeta = manifest.getScripts().get(0);
		assertEquals("s1", scriptMeta.getNodeId());
		assertEquals(4, scriptMeta.getVersion());
		assertEquals("groovy", scriptMeta.getLanguage());
		verify(session).close();
	}

	@Test
	void fetchManifestOnStandaloneAddsReplicaSetHint() {
		ClientSession session = mock(ClientSession.class);
		when(client.startSession(any(ClientSessionOptions.class))).thenReturn(session);
		when(session.withTransaction(any(TransactionBody.class), any())).thenThrow(
				new MongoException(20, "Transaction numbers are only allowed on a replica set member or mongos"));

		RuleStorageException error = assertThrows(RuleStorageException.class, () -> repository().fetchManifest());
		assertTrue(error.getMessage().contains("replica set or sharded cluster"));
		verify(session).close();
	}

	@Test
	void fetchManifestRethrowsUnrelatedMongoErrors() {
		ClientSession session = mock(ClientSession.class);
		when(client.startSession(any(ClientSessionOptions.class))).thenReturn(session);
		when(session.withTransaction(any(TransactionBody.class), any()))
				.thenThrow(new MongoException(5, "boom"));

		assertThrows(MongoException.class, () -> repository().fetchManifest());
	}

	@Test
	void fetchChainMetaAndScriptMetaMapProjections() {
		stubFindFirst(chains, new Document("chainId", "c1").append("version", 3L).append("contentMd5", "m"));
		ChainMeta chainMeta = repository().fetchChainMeta("c1");
		assertEquals("c1", chainMeta.getChainId());
		assertEquals(3, chainMeta.getVersion());

		stubFindFirst(chains, null);
		assertNull(repository().fetchChainMeta("missing"));

		stubFindFirst(scripts, new Document("nodeId", "s1")
				.append("version", 1L).append("contentMd5", "m").append("type", "script")
				.append("language", "groovy").append("name", "s1"));
		ScriptMeta scriptMeta = repository().fetchScriptMeta("s1");
		assertEquals("s1", scriptMeta.getNodeId());
		assertEquals("script", scriptMeta.getType());

		stubFindFirst(scripts, null);
		assertNull(repository().fetchScriptMeta("missing"));
	}

	@Test
	void fetchScriptMapsDocumentAndHandlesMissing() {
		stubFindFirst(scripts, new Document("nodeId", "s1").append("script", "return 1")
				.append("name", "s1").append("type", "script").append("language", "groovy")
				.append("version", 2L).append("contentMd5", "m").append("enable", true));

		ScriptRecord record = repository().fetchScript("s1");
		assertEquals("return 1", record.getScript());
		assertEquals(2, record.getVersion());
		assertTrue(record.isEnable());

		stubFindFirst(scripts, null);
		assertNull(repository().fetchScript("missing"));
	}

	@Test
	void fetchChainToleratesMissingOrMalformedVersion() {
		stubFindFirst(chains, new Document("chainId", "c1").append("el", "THEN(a)")
				.append("version", "not-a-number").append("enable", false));
		assertEquals(0, repository().fetchChain("c1").getVersion());
		assertFalse(repository().fetchChain("c1").isEnable());

		stubFindFirst(chains, new Document("chainId", "c2").append("el", "THEN(b)"));
		assertEquals(0, repository().fetchChain("c2").getVersion());
	}

	@Test
	void fetchLatestSeqReadsSequenceDocument() {
		stubFindFirst(sequences, new Document("seq", 11L));
		assertEquals(11, repository().fetchLatestSeq());

		stubFindFirst(sequences, null);
		assertEquals(0, repository().fetchLatestSeq());
	}

	private MongoRuleRepository repository() {
		return new MongoRuleRepository(client, database, new MongoCollections("lf_"), "app");
	}

	private ClientSession transactionSession() {
		ClientSession session = mock(ClientSession.class);
		when(client.startSession(any(ClientSessionOptions.class))).thenReturn(session);
		when(session.withTransaction(any(TransactionBody.class), any())).thenAnswer(invocation -> {
			TransactionBody<?> body = invocation.getArgument(0);
			return body.execute();
		});
		return session;
	}

	private void stubFindFirst(MongoCollection<Document> collection, Document first) {
		FindIterable<Document> iterable = mock(FindIterable.class);
		when(iterable.projection(any(Bson.class))).thenReturn(iterable);
		when(iterable.first()).thenReturn(first);
		when(collection.find(any(Bson.class))).thenReturn(iterable);
	}

	private void stubSessionFindFirst(MongoCollection<Document> collection, Document first) {
		FindIterable<Document> iterable = mock(FindIterable.class);
		when(iterable.projection(any(Bson.class))).thenReturn(iterable);
		when(iterable.first()).thenReturn(first);
		when(collection.find(any(ClientSession.class), any(Bson.class))).thenReturn(iterable);
	}

	private void stubSessionForEach(MongoCollection<Document> collection, Document... documents) {
		FindIterable<Document> iterable = mock(FindIterable.class);
		when(iterable.projection(any(Bson.class))).thenReturn(iterable);
		doAnswer(invocation -> {
			Consumer<Document> consumer = invocation.getArgument(0);
			for (Document document : documents) {
				consumer.accept(document);
			}
			return null;
		}).when(iterable).forEach(any(Consumer.class));
		when(collection.find(any(ClientSession.class), any(Bson.class))).thenReturn(iterable);
	}
}
