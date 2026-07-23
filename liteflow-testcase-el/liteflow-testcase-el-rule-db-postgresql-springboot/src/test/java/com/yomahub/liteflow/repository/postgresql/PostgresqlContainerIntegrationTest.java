package com.yomahub.liteflow.repository.postgresql;

import com.yomahub.liteflow.property.RuleDbPostgresqlConfig;
import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.PublishResult;
import com.yomahub.liteflow.publisher.PublishScriptRequest;
import com.yomahub.liteflow.publisher.RemoveRuleRequest;
import com.yomahub.liteflow.publisher.RulePublisher;
import com.yomahub.liteflow.publisher.exception.VersionConflictException;
import com.yomahub.liteflow.repository.ChangeSourceHealth;
import com.yomahub.liteflow.repository.vo.ChainMeta;
import com.yomahub.liteflow.repository.vo.ChainRecord;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import com.yomahub.liteflow.repository.vo.RuleManifest;
import com.yomahub.liteflow.repository.vo.ScriptMeta;
import com.yomahub.liteflow.repository.vo.ScriptRecord;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** End-to-end coverage against a real PostgreSQL instance. Skipped when Docker is unavailable. */
@Testcontainers(disabledWithoutDocker = true)
class PostgresqlContainerIntegrationTest {

	@Container
	private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:15-alpine");

	@Test
	void autoInitRunsTheBundledDdlAndReadsAnEmptyManifest() {
		PostgresqlRuleRepository repository = repository("it-init", true);

		RuleManifest manifest = repository.fetchManifest();
		assertEquals(0, manifest.getChains().size());
		assertEquals(0, manifest.getScripts().size());
		assertEquals(0, manifest.getLatestSeq());
		assertEquals(0, repository.fetchLatestSeq());

		// The schema created above also satisfies a repository that does not auto-init.
		PostgresqlRuleRepository verified = repository("it-init", false);
		assertEquals(0, verified.fetchManifest().getLatestSeq());
	}

	@Test
	void publishConflictRemoveAndReadBackAcrossTheRealDatabase() {
		String application = "it-crud";
		PostgresqlRuleRepository repository = repository(application, true);
		try (RulePublisher publisher = publisher(application)) {
			PublishResult created = publisher.publishChain(PublishChainRequest.builder()
					.chainId("orderChain").el("THEN(a, b)").namespace("demo").expectedVersion(0L).build());
			assertEquals(1, created.getVersion());
			assertTrue(created.getSequence() > 0);

			PublishResult updated = publisher.publishChain(PublishChainRequest.builder()
					.chainId("orderChain").el("THEN(a, c)").namespace("demo").expectedVersion(1L).build());
			assertEquals(2, updated.getVersion());

			assertThrows(VersionConflictException.class, () -> publisher.publishChain(
					PublishChainRequest.builder().chainId("orderChain").el("THEN(x)").expectedVersion(1L).build()));

			PublishResult script = publisher.publishScript(PublishScriptRequest.builder()
					.nodeId("s1").script("return true").name("s1").type("script").language("groovy")
					.expectedVersion(0L).build());
			assertEquals(1, script.getVersion());
		}

		ChainRecord chain = repository.fetchChain("orderChain");
		assertNotNull(chain);
		assertEquals("THEN(a, c)", chain.getEl());
		assertEquals("demo", chain.getNamespace());
		assertEquals(2, chain.getVersion());
		assertTrue(chain.isEnable());

		ChainMeta chainMeta = repository.fetchChainMeta("orderChain");
		assertNotNull(chainMeta);
		assertEquals(2, chainMeta.getVersion());
		assertNull(repository.fetchChainMeta("missing"));

		ScriptRecord script = repository.fetchScript("s1");
		assertNotNull(script);
		assertEquals("return true", script.getScript());
		assertEquals("groovy", script.getLanguage());
		assertNull(repository.fetchScript("missing"));

		ScriptMeta scriptMeta = repository.fetchScriptMeta("s1");
		assertNotNull(scriptMeta);
		assertEquals(1, scriptMeta.getVersion());
		assertNull(repository.fetchScriptMeta("missing"));

		RuleManifest manifest = repository.fetchManifest();
		assertEquals(1, manifest.getChains().size());
		assertEquals(1, manifest.getScripts().size());
		assertTrue(manifest.getLatestSeq() >= 3);

		try (RulePublisher publisher = publisher(application)) {
			PublishResult removed = publisher.removeChain(RemoveRuleRequest.builder()
					.targetId("orderChain").expectedVersion(2L).build());
			assertEquals(2, removed.getVersion());
			assertEquals(ChangeRecord.Op.DELETE, removed.getOperation());

			// Removing an absent target appends a version-0 tombstone change.
			PublishResult tombstone = publisher.removeChain(RemoveRuleRequest.builder()
					.targetId("neverExisted").build());
			assertEquals(0, tombstone.getVersion());

			publisher.removeScript(RemoveRuleRequest.builder().targetId("s1").expectedVersion(1L).build());
		}

		assertNull(repository.fetchChain("orderChain"));
		assertNull(repository.fetchScript("s1"));
		assertFalse(repository.fetchManifest().getChains().stream()
				.anyMatch(meta -> "orderChain".equals(meta.getChainId())));
	}

	@Test
	void pollingDeliversPublishedChangesInSequenceOrder() {
		String application = "it-poll";
		PostgresqlRuleRepository repository = repository(application, true);
		long baseline = repository.fetchLatestSeq();
		try (RulePublisher publisher = publisher(application)) {
			publisher.publishChain(PublishChainRequest.builder().chainId("c1").el("THEN(a)").build());
			publisher.publishScript(PublishScriptRequest.builder().nodeId("s1").script("return 1")
					.type("script").language("groovy").build());
			publisher.removeChain(RemoveRuleRequest.builder().targetId("c1").build());
		}

		List<ChangeRecord> delivered = new ArrayList<>();
		PostgresqlPollingChangeSource source = new PostgresqlPollingChangeSource(repository, 3600, 2);
		source.open(delivered::addAll);
		source.activate(baseline);
		try {
			source.pollOnce();

			assertEquals(3, delivered.size());
			assertEquals("c1", delivered.get(0).getTargetId());
			assertEquals("s1", delivered.get(1).getTargetId());
			assertEquals(ChangeRecord.Op.DELETE, delivered.get(2).getOp());
			for (int i = 1; i < delivered.size(); i++) {
				assertTrue(delivered.get(i).getSeq() > delivered.get(i - 1).getSeq());
			}
			assertEquals(ChangeSourceHealth.Status.UP, source.health().getStatus());
			assertEquals(repository.fetchLatestSeq(), source.health().getCursor());
		}
		finally {
			source.close();
		}
	}

	@Test
	void laterPublicationCannotCommitBeforeTheLowerSequence() throws Exception {
		String application = "it-commit-order";
		repository(application, true);
		PGSimpleDataSource delegate = new PGSimpleDataSource();
		delegate.setURL(POSTGRES.getJdbcUrl());
		delegate.setUser(POSTGRES.getUsername());
		delegate.setPassword(POSTGRES.getPassword());
		CountDownLatch firstCommitEntered = new CountDownLatch(1);
		CountDownLatch allowFirstCommit = new CountDownLatch(1);
		DataSource dataSource = blockingFirstCommit(delegate, firstCommitEntered, allowFirstCommit);
		ExecutorService executor = Executors.newFixedThreadPool(2);
		try (RulePublisher firstPublisher = publisher(application, dataSource);
				RulePublisher secondPublisher = publisher(application, dataSource)) {
			Future<PublishResult> first = executor.submit(() -> firstPublisher.publishChain(
					PublishChainRequest.builder().chainId("first").el("THEN(a)").expectedVersion(0L).build()));
			assertTrue(firstCommitEntered.await(5, TimeUnit.SECONDS), "first publication did not reach commit");

			Future<PublishResult> second = executor.submit(() -> secondPublisher.publishChain(
					PublishChainRequest.builder().chainId("second").el("THEN(a)").expectedVersion(0L).build()));
			assertThrows(TimeoutException.class, () -> second.get(250, TimeUnit.MILLISECONDS),
					"later publication must wait for the transaction holding the publishing-order lock");

			allowFirstCommit.countDown();
			PublishResult firstResult = first.get(5, TimeUnit.SECONDS);
			PublishResult secondResult = second.get(5, TimeUnit.SECONDS);
			assertTrue(firstResult.getSequence() < secondResult.getSequence());
		}
		finally {
			allowFirstCommit.countDown();
			executor.shutdownNow();
		}
	}

	private PostgresqlRuleRepository repository(String applicationName, boolean autoInit) {
		RuleDbPostgresqlConfig config = new RuleDbPostgresqlConfig();
		config.setUrl(POSTGRES.getJdbcUrl());
		config.setUsername(POSTGRES.getUsername());
		config.setPassword(POSTGRES.getPassword());
		config.setAutoInitTable(autoInit);
		return new PostgresqlRuleRepository(new PostgresqlConnectionManager(config),
				new PostgresqlDialect("lf_"), applicationName, autoInit);
	}

	private RulePublisher publisher(String applicationName) {
		return new PostgresqlRulePublisherProvider().create(PostgresqlPublisherConfig.builder()
				.applicationName(applicationName).url(POSTGRES.getJdbcUrl())
				.username(POSTGRES.getUsername()).password(POSTGRES.getPassword()).build());
	}

	private RulePublisher publisher(String applicationName, DataSource dataSource) {
		return new PostgresqlRulePublisherProvider().create(PostgresqlPublisherConfig.builder()
				.applicationName(applicationName).dataSource(dataSource).build());
	}

	private DataSource blockingFirstCommit(DataSource delegate, CountDownLatch entered, CountDownLatch release) {
		AtomicInteger borrowed = new AtomicInteger();
		return (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(),
				new Class<?>[] { DataSource.class }, (proxy, method, args) -> {
					Object value = invoke(method, delegate, args);
					if (value instanceof Connection) {
						return blockingCommit((Connection) value, borrowed.incrementAndGet() == 1, entered, release);
					}
					return value;
				});
	}

	private Connection blockingCommit(Connection delegate, boolean block, CountDownLatch entered,
			CountDownLatch release) {
		return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
				new Class<?>[] { Connection.class }, (proxy, method, args) -> {
					if (block && "commit".equals(method.getName())) {
						entered.countDown();
						try {
							if (!release.await(5, TimeUnit.SECONDS)) {
								throw new SQLException("timed out waiting to release the first commit");
							}
						}
						catch (InterruptedException e) {
							Thread.currentThread().interrupt();
							throw new SQLException("interrupted while waiting to commit", e);
						}
					}
					return invoke(method, delegate, args);
				});
	}

	private Object invoke(java.lang.reflect.Method method, Object target, Object[] args) throws Throwable {
		try {
			return method.invoke(target, args);
		}
		catch (InvocationTargetException e) {
			throw e.getCause();
		}
	}
}
