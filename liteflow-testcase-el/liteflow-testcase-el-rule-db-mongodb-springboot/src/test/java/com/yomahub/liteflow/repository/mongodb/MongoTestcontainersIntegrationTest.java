package com.yomahub.liteflow.repository.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.yomahub.liteflow.property.LiteflowConfig;
import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.property.RuleDbMongoConfig;
import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.PublishResult;
import com.yomahub.liteflow.publisher.RulePublisher;
import com.yomahub.liteflow.publisher.exception.RuleStorageException;
import com.yomahub.liteflow.publisher.exception.VersionConflictException;
import com.yomahub.liteflow.repository.ChangeSourceHealth;
import com.yomahub.liteflow.repository.vo.ChainRecord;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import com.yomahub.liteflow.repository.vo.RuleManifest;
import com.yomahub.liteflow.repository.vo.ScriptRecord;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end coverage against the replica set initialized by MongoDBContainer.
 */
@Testcontainers(disabledWithoutDocker = true)
class MongoTestcontainersIntegrationTest {

	@Container
	static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:6.0.14"));

	private MongoClient client;
	private MongoDatabase database;

	@BeforeEach
	void setUp() {
		client = MongoClients.create(MONGO.getConnectionString());
		database = client.getDatabase("itrules");
		database.drop();
	}

	@AfterEach
	void tearDown() {
		client.close();
	}

	@Test
	void crudAndPollingWorkAgainstRealMongo() {
		MongoCollections names = new MongoCollections("lf_");
		MongoSchema.ensureIndexes(database, names);
		MongoSchema.ensureIndexes(database, names);

		database.getCollection(names.chains()).insertOne(new Document("_id", id("c1"))
				.append("applicationName", "it-app").append("chainId", "c1").append("el", "THEN(a)")
				.append("route", "AND(a)").append("namespace", "ns").append("version", 1L)
				.append("contentMd5", "cm").append("enable", true));
		database.getCollection(names.scripts()).insertOne(new Document("_id", id("s1"))
				.append("applicationName", "it-app").append("nodeId", "s1").append("script", "return 1")
				.append("name", "s1").append("type", "script").append("language", "groovy")
				.append("version", 2L).append("contentMd5", "sm").append("enable", true));
		database.getCollection(names.sequences()).insertOne(new Document("_id", "it-app").append("seq", 2L));
		database.getCollection(names.changes()).insertMany(Arrays.asList(
				change(1, "CHAIN", "c1", "UPSERT", 1),
				change(2, "SCRIPT", "s1", "DELETE", 2)));

		MongoRuleRepository repository = new MongoRuleRepository(client, database, names, "it-app");
		ChainRecord chain = repository.fetchChain("c1");
		assertEquals("THEN(a)", chain.getEl());
		assertEquals("AND(a)", chain.getRoute());
		assertEquals(1, chain.getVersion());
		assertNotNull(repository.fetchChainMeta("c1"));
		ScriptRecord script = repository.fetchScript("s1");
		assertEquals("return 1", script.getScript());
		assertNotNull(repository.fetchScriptMeta("s1"));
		assertEquals(2, repository.fetchLatestSeq());
		List<ChangeRecord> records = repository.fetchChangesSince(0, 10);
		assertEquals(2, records.size());
		assertEquals(ChangeRecord.Op.DELETE, records.get(1).getOp());

		MongoPollingChangeSource source = new MongoPollingChangeSource(repository, 3600, 1);
		List<ChangeRecord> delivered = new CopyOnWriteArrayList<>();
		source.open(delivered::addAll);
		source.activate(0);
		try {
			source.pollOnce();
			assertEquals(2, delivered.size());
			assertEquals(2, source.health().getCursor());
			assertEquals(ChangeSourceHealth.Status.UP, source.health().getStatus());
		}
		finally {
			source.close();
		}
	}

	@Test
	void providerBootsAgainstRealMongoAndClosesCleanly() {
		LiteflowConfig liteflowConfig = new LiteflowConfig();
		RuleDbConfig ruleDb = new RuleDbConfig();
		ruleDb.setApplicationName("it-provider");
		RuleDbMongoConfig mongodb = new RuleDbMongoConfig();
		mongodb.setUri(MONGO.getConnectionString());
		mongodb.setDatabase("itprovider");
		ruleDb.setMongodb(mongodb);
		liteflowConfig.setRuleDb(ruleDb);
		LiteflowConfigGetter.setLiteflowConfig(liteflowConfig);
		try {
			MongoRuleDbProvider provider = new MongoRuleDbProvider();
			try {
				assertEquals("mongodb", provider.type());
				assertNotNull(provider.repository());
				assertNotNull(provider.changeSource());
				assertEquals(ChangeSourceHealth.Status.STARTING, provider.changeSource().health().getStatus());
			}
			finally {
				provider.close();
			}
		}
		finally {
			LiteflowConfigGetter.clean();
		}
	}

	@Test
	void publisherCommitsTransactionsAndManifestReadsOneSnapshot() {
		MongoCollections names = new MongoCollections("lf_");
		try (RulePublisher publisher = new MongoRulePublisherProvider().create(publisherConfig("tx-app"))) {
			PublishResult created = publisher.publishChain(PublishChainRequest.builder()
					.chainId("c1").el("THEN(a)").expectedVersion(0L).build());
			PublishResult updated = publisher.publishChain(PublishChainRequest.builder()
					.chainId("c1").el("THEN(a, b)").expectedVersion(1L).build());

			assertEquals(1, created.getVersion());
			assertEquals(2, updated.getVersion());
			assertTrue(updated.getSequence() > created.getSequence());
		}

		MongoRuleRepository repository = new MongoRuleRepository(client, database, names, "tx-app");
		RuleManifest manifest = repository.fetchManifest();
		assertEquals(1, manifest.getChains().size());
		assertEquals(2, manifest.getChains().get(0).getVersion());
		assertEquals(2, manifest.getLatestSeq());
		assertEquals("THEN(a, b)", repository.fetchChain("c1").getEl());
	}

	@Test
	void transactionRollsBackRuleAndSequenceWhenChangeLogWriteFails() {
		MongoCollections names = new MongoCollections("lf_");
		MongoSchema.ensureIndexes(database, names);
		database.getCollection(names.changes()).insertOne(change("rollback-app", 1,
				"CHAIN", "occupied", "UPSERT", 1));

		try (RulePublisher publisher = new MongoRulePublisherProvider().create(publisherConfig("rollback-app"))) {
			assertThrows(RuleStorageException.class, () -> publisher.publishChain(PublishChainRequest.builder()
					.chainId("c1").el("THEN(a)").build()));
		}

		assertEquals(0, database.getCollection(names.chains()).countDocuments());
		assertEquals(0, database.getCollection(names.sequences()).countDocuments());
		assertEquals(1, database.getCollection(names.changes()).countDocuments());
	}

	@Test
	void concurrentExpectedVersionZeroAllowsExactlyOneCommit() throws Exception {
		MongoCollections names = new MongoCollections("lf_");
		try (RulePublisher publisher = new MongoRulePublisherProvider().create(publisherConfig("race-app"))) {
			ExecutorService executor = Executors.newFixedThreadPool(2);
			CountDownLatch ready = new CountDownLatch(2);
			CountDownLatch start = new CountDownLatch(1);
			try {
				Future<Object> first = executor.submit(() -> publishAtStart(publisher, ready, start));
				Future<Object> second = executor.submit(() -> publishAtStart(publisher, ready, start));
				ready.await();
				start.countDown();

				Object firstResult = first.get();
				Object secondResult = second.get();
				assertTrue(firstResult instanceof PublishResult || secondResult instanceof PublishResult);
				assertTrue(firstResult instanceof VersionConflictException
						|| secondResult instanceof VersionConflictException);
			}
			finally {
				executor.shutdownNow();
			}
		}

		assertEquals(1, database.getCollection(names.chains()).countDocuments());
		assertEquals(1, database.getCollection(names.changes()).countDocuments());
		assertEquals(1L, database.getCollection(names.sequences())
				.find(new Document("_id", "race-app")).first().getLong("seq"));
	}

	private Object publishAtStart(RulePublisher publisher, CountDownLatch ready, CountDownLatch start)
			throws InterruptedException {
		ready.countDown();
		start.await();
		try {
			return publisher.publishChain(PublishChainRequest.builder()
					.chainId("same").el("THEN(a)").expectedVersion(0L).build());
		}
		catch (VersionConflictException e) {
			return e;
		}
	}

	private MongoPublisherConfig publisherConfig(String applicationName) {
		return MongoPublisherConfig.builder().applicationName(applicationName)
				.database(database.getName()).collectionPrefix("lf_").mongoClient(client).build();
	}

	private Document id(String targetId) {
		return new Document("applicationName", "it-app").append("targetId", targetId);
	}

	private Document change(long seq, String type, String targetId, String operation, long version) {
		return change("it-app", seq, type, targetId, operation, version);
	}

	private Document change(String applicationName, long seq, String type, String targetId,
			String operation, long version) {
		return new Document("_id", new Document("applicationName", applicationName).append("seq", seq))
				.append("applicationName", applicationName).append("seq", seq).append("targetType", type)
				.append("targetId", targetId).append("operation", operation).append("version", version);
	}
}
