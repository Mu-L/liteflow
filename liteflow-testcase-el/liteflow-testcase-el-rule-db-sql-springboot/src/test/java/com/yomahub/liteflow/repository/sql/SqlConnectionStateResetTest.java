package com.yomahub.liteflow.repository.sql;

import com.yomahub.liteflow.property.LiteflowConfig;
import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.RemoveRuleRequest;
import com.yomahub.liteflow.publisher.RulePublisher;
import com.yomahub.liteflow.publisher.RulePublisherFactory;
import com.yomahub.liteflow.publisher.exception.RuleStorageException;
import com.yomahub.liteflow.publisher.exception.VersionConflictException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 借出连接归还前的事务状态复位回归：autoCommit/隔离级别必须在 close 前恢复，
 * 否则池化连接会污染下一个借用者；同事务体内 RuntimeException 也必须触发回滚。
 */
class SqlConnectionStateResetTest {

	private static final String SPY_URL_PREFIX = "jdbc:sql-state-spy:";

	private static final Map<String, List<String>> EVENTS = new ConcurrentHashMap<>();

	private static volatile String failOnSqlContaining;

	@BeforeAll
	static void registerSpyDriver() throws SQLException {
		DriverManager.registerDriver(new SpyDriver());
	}

	@AfterAll
	static void deregisterSpyDriver() throws SQLException {
		DriverManager.deregisterDriver(DriverManager.getDriver(SPY_URL_PREFIX + "x"));
	}

	@AfterEach
	void cleanInstalledConfig() {
		LiteflowConfigGetter.clean();
		failOnSqlContaining = null;
	}

	@Test
	void publisherImplRestoresAutoCommitAfterCommitBeforeClose() throws Exception {
		DataSource dataSource = mock(DataSource.class);
		Connection connection = mock(Connection.class);
		when(dataSource.getConnection()).thenReturn(connection);
		when(connection.getAutoCommit()).thenReturn(true);
		stubPublishingLock(connection);
		PreparedStatement insert = mock(PreparedStatement.class);
		PreparedStatement changeLog = mock(PreparedStatement.class);
		when(connection.prepareStatement(anyString())).thenReturn(insert);
		when(connection.prepareStatement(anyString(), anyInt())).thenReturn(changeLog);
		when(insert.executeUpdate()).thenReturn(1);
		ResultSet generatedKeys = mock(ResultSet.class);
		when(generatedKeys.next()).thenReturn(true);
		when(generatedKeys.getLong(1)).thenReturn(1L);
		when(changeLog.getGeneratedKeys()).thenReturn(generatedKeys);

		try (RulePublisher publisher = new SqlRulePublisherProvider().create(
				SqlPublisherConfig.builder().applicationName("app")
						.dataSource(dataSource).tablePrefix("lf_").build())) {
			publisher.publishChain(PublishChainRequest.builder()
					.chainId("c1").el("THEN(a)").expectedVersion(0L).build());
		}

		InOrder order = inOrder(connection);
		order.verify(connection).setAutoCommit(false);
		order.verify(connection).commit();
		order.verify(connection).setAutoCommit(true);
		order.verify(connection).close();
	}

	@Test
	void publisherImplRestoresAutoCommitAfterRollbackBeforeClose() throws Exception {
		DataSource dataSource = mock(DataSource.class);
		Connection connection = mock(Connection.class);
		when(dataSource.getConnection()).thenReturn(connection);
		when(connection.getAutoCommit()).thenReturn(true);
		stubPublishingLock(connection);
		PreparedStatement insert = mock(PreparedStatement.class);
		when(connection.prepareStatement(anyString())).thenReturn(insert);
		when(insert.executeUpdate()).thenThrow(new SQLException("database unavailable", "08006"));

		try (RulePublisher publisher = new SqlRulePublisherProvider().create(
				SqlPublisherConfig.builder().applicationName("app")
						.dataSource(dataSource).tablePrefix("lf_").build())) {
			Assertions.assertThrows(RuleStorageException.class,
					() -> publisher.publishChain(PublishChainRequest.builder()
							.chainId("c1").el("THEN(a)").expectedVersion(0L).build()));
		}

		InOrder order = inOrder(connection);
		order.verify(connection).setAutoCommit(false);
		order.verify(connection).rollback();
		order.verify(connection).setAutoCommit(true);
		order.verify(connection).close();
	}

	@Test
	void fetchManifestRestoresAutoCommitAndIsolationBeforeClose() throws Exception {
		String db = "state-reset-repo";
		Connection real = h2(db);
		int originalIsolation = real.getTransactionIsolation();
		List<String> events = new ArrayList<>();
		Connection proxy = recordingProxy(real, events);
		SqlConnectionManager connectionManager = new SqlConnectionManager() {
			@Override
			public Connection getConnection() {
				return proxy;
			}
		};
		SqlRuleRepository repository = new SqlRuleRepository(connectionManager,
				new SqlDialect("lf_"), "state-reset-app", true);

		repository.fetchManifest();

		assertOrder(events, "commit", "setAutoCommit(true)", "close");
		int isolationResetAt = events.indexOf("setTransactionIsolation(" + originalIsolation + ")");
		int commitAt = events.indexOf("commit");
		int closeAt = events.indexOf("close");
		Assertions.assertTrue(isolationResetAt > commitAt && isolationResetAt < closeAt,
				"isolation must be restored after commit and before close: " + events);
	}

	@Test
	void legacyPublisherRestoresAutoCommitAfterCommitBeforeClose() throws Exception {
		String db = "legacy-commit";
		installLegacyConfig(db);
		createTables(db);

		new SqlRulePublisher().publishChain("legacyStateChain", "THEN(a)");

		assertOrder(EVENTS.get(db), "commit", "setAutoCommit(true)", "close");
	}

	@Test
	void legacyPublisherRollsBackWhenTransactionBodyThrowsRuntimeException() throws Exception {
		String db = "legacy-runtime";
		installLegacyConfig(db);
		createTables(db);
		failOnSqlContaining = "INSERT INTO lf_change_log";

		SqlRulePublisher publisher = new SqlRulePublisher();
		IllegalStateException error = Assertions.assertThrows(IllegalStateException.class,
				() -> publisher.publishChain("legacyRuntimeChain", "THEN(a)"));
		Assertions.assertEquals("boom", error.getMessage());

		List<String> events = EVENTS.get(db);
		Assertions.assertFalse(events.contains("commit"),
				"failed transaction must not commit: " + events);
		assertOrder(events, "rollback()", "setAutoCommit(true)", "close");
	}

	@Test
	void removeConflictMessageDistinguishesMissingTargetFromVersionMismatch() throws Exception {
		String db = "state-message";
		createTables(db);
		try (RulePublisher publisher = RulePublisherFactory.create(SqlPublisherConfig.builder()
				.applicationName("state-message-app")
				.url(h2Url(db)).username("sa").password("").build())) {
			VersionConflictException missing = Assertions.assertThrows(VersionConflictException.class,
					() -> publisher.removeChain(RemoveRuleRequest.builder()
							.targetId("ghostChain").expectedVersion(0L).build()));
			Assertions.assertTrue(missing.getMessage().contains("ghostChain"));
			Assertions.assertTrue(missing.getMessage().contains("does not exist"),
					"missing target must be reported as absent, not as a version match: "
							+ missing.getMessage());

			publisher.publishChain(PublishChainRequest.builder()
					.chainId("mismatchChain").el("THEN(a)").expectedVersion(0L).build());
			VersionConflictException mismatch = Assertions.assertThrows(VersionConflictException.class,
					() -> publisher.removeChain(RemoveRuleRequest.builder()
							.targetId("mismatchChain").expectedVersion(99L).build()));
			Assertions.assertTrue(mismatch.getMessage()
							.contains("expected version[99] but current version is[1]"),
					"version mismatch must keep the expected/current detail: " + mismatch.getMessage());
		}
	}

	private void assertOrder(List<String> events, String... expected) {
		Assertions.assertNotNull(events, "no events recorded");
		int previous = -1;
		for (String event : expected) {
			int at = events.indexOf(event);
			Assertions.assertTrue(at > previous,
					"event[" + event + "] missing or out of order in " + events);
			previous = at;
		}
	}

	private void stubPublishingLock(Connection connection) throws Exception {
		Statement statement = mock(Statement.class);
		ResultSet row = mock(ResultSet.class);
		when(connection.createStatement()).thenReturn(statement);
		when(statement.executeQuery(anyString())).thenReturn(row);
		when(row.next()).thenReturn(true);
	}

	private void installLegacyConfig(String db) {
		RuleDbConfig ruleDb = new RuleDbConfig();
		ruleDb.setApplicationName("legacy-state-app");
		ruleDb.getSql().setUrl(SPY_URL_PREFIX + db);
		ruleDb.getSql().setUsername("sa");
		ruleDb.getSql().setPassword("");
		LiteflowConfig config = new LiteflowConfig();
		config.setRuleDb(ruleDb);
		LiteflowConfigGetter.setLiteflowConfig(config);
	}

	private void createTables(String db) throws Exception {
		try (Connection connection = h2(db)) {
			new SqlDialect("lf_").createTablesIfAbsent(connection);
		}
	}

	private Connection h2(String db) throws SQLException {
		return DriverManager.getConnection(h2Url(db), "sa", "");
	}

	private String h2Url(String db) {
		return "jdbc:h2:mem:" + db + ";DB_CLOSE_DELAY=-1;MODE=MySQL";
	}

	private static Connection recordingProxy(Connection real, List<String> events) {
		return (Connection) Proxy.newProxyInstance(SqlConnectionStateResetTest.class.getClassLoader(),
				new Class[] { Connection.class }, (proxy, method, args) -> {
					String name = method.getName();
					if (("setAutoCommit".equals(name) || "setTransactionIsolation".equals(name))
							&& args != null && args.length == 1) {
						events.add(name + "(" + args[0] + ")");
					}
					else if ("commit".equals(name) || "close".equals(name)) {
						events.add(name);
					}
					else if ("rollback".equals(name)) {
						events.add(args == null ? "rollback()" : "rollback(savepoint)");
					}
					if (failOnSqlContaining != null && "prepareStatement".equals(name)
							&& args != null && args.length > 0
							&& args[0].toString().contains(failOnSqlContaining)) {
						throw new IllegalStateException("boom");
					}
					try {
						return method.invoke(real, args);
					}
					catch (InvocationTargetException e) {
						throw e.getCause();
					}
				});
	}

	/** 将 {@code jdbc:sql-state-spy:<db>} 桥接到真实 H2 内存库，返回记录事件的代理连接。 */
	private static class SpyDriver implements Driver {

		@Override
		public Connection connect(String url, Properties info) throws SQLException {
			if (!acceptsURL(url)) {
				return null;
			}
			String db = url.substring(SPY_URL_PREFIX.length());
			Connection real = DriverManager.getConnection(
					"jdbc:h2:mem:" + db + ";DB_CLOSE_DELAY=-1;MODE=MySQL", "sa", "");
			List<String> events = new ArrayList<>();
			EVENTS.put(db, events);
			return recordingProxy(real, events);
		}

		@Override
		public boolean acceptsURL(String url) {
			return url != null && url.startsWith(SPY_URL_PREFIX);
		}

		@Override
		public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
			return new DriverPropertyInfo[0];
		}

		@Override
		public int getMajorVersion() {
			return 1;
		}

		@Override
		public int getMinorVersion() {
			return 0;
		}

		@Override
		public boolean jdbcCompliant() {
			return false;
		}

		@Override
		public Logger getParentLogger() throws SQLFeatureNotSupportedException {
			throw new SQLFeatureNotSupportedException();
		}
	}
}
