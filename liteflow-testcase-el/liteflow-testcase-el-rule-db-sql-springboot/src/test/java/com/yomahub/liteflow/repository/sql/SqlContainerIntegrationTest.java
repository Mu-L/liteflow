package com.yomahub.liteflow.repository.sql;

import com.yomahub.liteflow.property.RuleDbSqlConfig;
import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.PublishResult;
import com.yomahub.liteflow.publisher.PublishScriptRequest;
import com.yomahub.liteflow.publisher.RemoveRuleRequest;
import com.yomahub.liteflow.publisher.RulePublisher;
import com.yomahub.liteflow.publisher.exception.VersionConflictException;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import com.yomahub.liteflow.repository.vo.RuleManifest;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** SQL protocol coverage against both supported production database families. */
@Testcontainers(disabledWithoutDocker = true)
class SqlContainerIntegrationTest {

	@Container
	private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.0")
			.withDatabaseName("liteflow").withUsername("liteflow").withPassword("liteflow");

	@Container
	private static final MariaDBContainer<?> MARIADB = new MariaDBContainer<>("mariadb:11.4")
			.withDatabaseName("liteflow").withUsername("liteflow").withPassword("liteflow");

	@Test
	void mysqlCrudPollingAndConflictUseBundledDdl() {
		verifyCrudAndPolling(MYSQL, "mysql");
	}

	@Test
	void mariadbCrudPollingAndConflictUseBundledDdl() {
		verifyCrudAndPolling(MARIADB, "mariadb");
	}

	@Test
	void mysqlPublishingSequenceFollowsCommitOrder() throws Exception {
		verifyCommitOrder(MYSQL, "mysql-order");
	}

	@Test
	void mariadbPublishingSequenceFollowsCommitOrder() throws Exception {
		verifyCommitOrder(MARIADB, "mariadb-order");
	}

	private void verifyCrudAndPolling(JdbcDatabaseContainer<?> database, String application) {
		SqlRuleRepository repository = repository(database, application, true);
		RuleManifest empty = repository.fetchManifest();
		assertTrue(empty.getChains().isEmpty());
		assertEquals(0, empty.getLatestSeq());

		try (RulePublisher publisher = publisher(database, application)) {
			PublishResult created = publisher.publishChain(PublishChainRequest.builder()
					.chainId("c1").el("THEN(a)").expectedVersion(0L).build());
			PublishResult updated = publisher.publishChain(PublishChainRequest.builder()
					.chainId("c1").el("THEN(a, b)").expectedVersion(1L).build());
			assertEquals(1, created.getVersion());
			assertEquals(2, updated.getVersion());
			assertTrue(updated.getSequence() > created.getSequence());
			assertThrows(VersionConflictException.class, () -> publisher.publishChain(
					PublishChainRequest.builder().chainId("c1").el("THEN(x)").expectedVersion(1L).build()));

			publisher.publishScript(PublishScriptRequest.builder().nodeId("s1").script("return 1")
					.name("script").type("script").language("groovy").expectedVersion(0L).build());
		}

		RuleManifest manifest = repository.fetchManifest();
		assertEquals(1, manifest.getChains().size());
		assertEquals(1, manifest.getScripts().size());
		assertEquals("THEN(a, b)", repository.fetchChain("c1").getEl());
		assertEquals("return 1", repository.fetchScript("s1").getScript());
		List<ChangeRecord> changes = repository.fetchChangesSince(0, 10);
		assertEquals(3, changes.size());
		for (int i = 1; i < changes.size(); i++) {
			assertTrue(changes.get(i).getSeq() > changes.get(i - 1).getSeq());
		}

		try (RulePublisher publisher = publisher(database, application)) {
			publisher.removeChain(RemoveRuleRequest.builder().targetId("c1").expectedVersion(2L).build());
			publisher.removeScript(RemoveRuleRequest.builder().targetId("s1").expectedVersion(1L).build());
		}
		assertNull(repository.fetchChain("c1"));
		assertNull(repository.fetchScript("s1"));
	}

	private void verifyCommitOrder(JdbcDatabaseContainer<?> database, String application) throws Exception {
		repository(database, application, true).fetchManifest();
		DriverManagerDataSource delegate = new DriverManagerDataSource(
				database.getJdbcUrl(), database.getUsername(), database.getPassword());
		delegate.setDriverClassName(database.getDriverClassName());
		CountDownLatch firstCommitEntered = new CountDownLatch(1);
		CountDownLatch allowFirstCommit = new CountDownLatch(1);
		DataSource dataSource = blockingFirstCommit(delegate, firstCommitEntered, allowFirstCommit);
		ExecutorService executor = Executors.newFixedThreadPool(2);
		try (RulePublisher firstPublisher = publisher(application, dataSource);
				RulePublisher secondPublisher = publisher(application, dataSource)) {
			Future<PublishResult> first = executor.submit(() -> firstPublisher.publishChain(
					PublishChainRequest.builder().chainId("first").el("THEN(a)").expectedVersion(0L).build()));
			assertTrue(firstCommitEntered.await(5, TimeUnit.SECONDS));

			Future<PublishResult> second = executor.submit(() -> secondPublisher.publishChain(
					PublishChainRequest.builder().chainId("second").el("THEN(a)").expectedVersion(0L).build()));
			assertThrows(TimeoutException.class, () -> second.get(300, TimeUnit.MILLISECONDS));

			allowFirstCommit.countDown();
			assertTrue(first.get(5, TimeUnit.SECONDS).getSequence()
					< second.get(5, TimeUnit.SECONDS).getSequence());
		}
		finally {
			allowFirstCommit.countDown();
			executor.shutdownNow();
		}
	}

	private SqlRuleRepository repository(JdbcDatabaseContainer<?> database, String application, boolean autoInit) {
		RuleDbSqlConfig config = new RuleDbSqlConfig();
		config.setUrl(database.getJdbcUrl());
		config.setUsername(database.getUsername());
		config.setPassword(database.getPassword());
		config.setDriverClassName(database.getDriverClassName());
		config.setAutoInitTable(autoInit);
		return new SqlRuleRepository(new SqlConnectionManager(config),
				new SqlDialect("lf_"), application, autoInit);
	}

	private RulePublisher publisher(JdbcDatabaseContainer<?> database, String application) {
		return new SqlRulePublisherProvider().create(SqlPublisherConfig.builder().applicationName(application)
				.url(database.getJdbcUrl()).username(database.getUsername()).password(database.getPassword())
				.driverClassName(database.getDriverClassName()).build());
	}

	private RulePublisher publisher(String application, DataSource dataSource) {
		return new SqlRulePublisherProvider().create(SqlPublisherConfig.builder()
				.applicationName(application).dataSource(dataSource).build());
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
