package com.yomahub.liteflow.repository.postgresql;

import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.repository.vo.ChainMeta;
import com.yomahub.liteflow.repository.vo.ChainRecord;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import com.yomahub.liteflow.repository.vo.RuleManifest;
import com.yomahub.liteflow.repository.vo.ScriptMeta;
import com.yomahub.liteflow.repository.vo.ScriptRecord;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Mock-driven coverage for the repository query paths. */
class PostgresqlRuleRepositoryMockTest {

	@Test
	void fetchChainMapsRowsAndReturnsNullWhenAbsent() throws Exception {
		PreparedStatement found = statement(resultSet(new Object[][] {
				{"c1", "THEN(a)", "ROUTE", "ns", 2L, "md5", true}}));
		PostgresqlRuleRepository repository = repository(mapOf("SELECT chain_id, el_data", found));

		ChainRecord record = repository.fetchChain("c1");
		assertEquals("c1", record.getChainId());
		assertEquals("THEN(a)", record.getEl());
		assertEquals("ROUTE", record.getRoute());
		assertEquals("ns", record.getNamespace());
		assertEquals(2, record.getVersion());
		assertEquals("md5", record.getMd5());
		assertTrue(record.isEnable());

		assertNull(repository.fetchChain("missing"));
		assertThrows(ConfigErrorException.class, () -> repository.fetchChain(" "));
	}

	@Test
	void fetchChainMetaMapsRowsAndReturnsNullWhenAbsent() throws Exception {
		PreparedStatement found = statement(resultSet(new Object[][] {{"c1", 4L, "md5"}}));
		PostgresqlRuleRepository repository = repository(mapOf("SELECT chain_id, version", found));

		ChainMeta meta = repository.fetchChainMeta("c1");
		assertEquals("c1", meta.getChainId());
		assertEquals(4, meta.getVersion());
		assertNull(repository.fetchChainMeta("missing"));
	}

	@Test
	void fetchScriptMapsRowsAndReturnsNullWhenAbsent() throws Exception {
		PreparedStatement found = statement(resultSet(new Object[][] {
				{"s1", "return 1", "s1", "script", "groovy", 3L, "md5", true}}));
		PostgresqlRuleRepository repository = repository(mapOf("SELECT node_id, script_data", found));

		ScriptRecord record = repository.fetchScript("s1");
		assertEquals("s1", record.getNodeId());
		assertEquals("return 1", record.getScript());
		assertEquals("groovy", record.getLanguage());
		assertEquals(3, record.getVersion());
		assertTrue(record.isEnable());
		assertNull(repository.fetchScript("missing"));
	}

	@Test
	void fetchScriptMetaMapsRowsAndReturnsNullWhenAbsent() throws Exception {
		PreparedStatement found = statement(resultSet(new Object[][] {
				{"s1", 7L, "md5", "script", "groovy", "s1"}}));
		PostgresqlRuleRepository repository = repository(mapOf("SELECT node_id, version", found));

		ScriptMeta meta = repository.fetchScriptMeta("s1");
		assertEquals("s1", meta.getNodeId());
		assertEquals(7, meta.getVersion());
		assertNull(repository.fetchScriptMeta("missing"));
	}

	@Test
	void fetchManifestAggregatesChainsScriptsAndLatestSeq() throws Exception {
		PreparedStatement chains = statement(resultSet(new Object[][] {{"c1", 1L, "m1"}, {"c2", 2L, "m2"}}));
		PreparedStatement scripts = statement(resultSet(new Object[][] {{"s1", 3L, "m3", "script", "groovy", "s1"}}));
		PreparedStatement latest = statement(resultSet(new Object[][] {{9L}}));
		PostgresqlRuleRepository repository = repository(
				mapOf("SELECT chain_id", chains, "SELECT node_id", scripts, "SELECT MAX(seq)", latest));

		RuleManifest manifest = repository.fetchManifest();
		assertEquals(2, manifest.getChains().size());
		assertEquals(1, manifest.getScripts().size());
		assertEquals(9, manifest.getLatestSeq());
	}

	@Test
	void fetchLatestSeqTreatsNullMaxAsZero() throws Exception {
		PreparedStatement latest = statement(resultSet(new Object[][] {{null}}));
		PostgresqlRuleRepository repository = repository(mapOf("SELECT MAX(seq)", latest));
		assertEquals(0, repository.fetchLatestSeq());
	}

	@Test
	void fetchChangesSinceMapsRowsAndRejectsNonPositiveLimits() throws Exception {
		PreparedStatement changes = statement(resultSet(new Object[][] {
				{5L, "CHAIN", "c1", "UPSERT", 1L},
				{6L, "SCRIPT", "s1", "DELETE", 0L}}));
		PostgresqlRuleRepository repository = repository(mapOf("SELECT seq,", changes));

		List<ChangeRecord> records = repository.fetchChangesSince(4, 10);
		assertEquals(2, records.size());
		assertEquals(5, records.get(0).getSeq());
		assertEquals(ChangeRecord.TargetType.CHAIN, records.get(0).getTargetType());
		assertEquals(ChangeRecord.Op.DELETE, records.get(1).getOp());
		assertEquals(0, records.get(1).getVersion());

		assertThrows(ConfigErrorException.class, () -> repository.fetchChangesSince(0, 0));
	}

	@Test
	void corruptedChangeLogRowRaisesCorruptionException() throws Exception {
		PreparedStatement changes = statement(resultSet(new Object[][] {{5L, "BOGUS", "c1", "UPSERT", 1L}}));
		PostgresqlRuleRepository repository = repository(mapOf("SELECT seq,", changes));
		PostgresqlChangeLogCorruptionException error = assertThrows(
				PostgresqlChangeLogCorruptionException.class, () -> repository.fetchChangesSince(0, 10));
		assertTrue(error.getMessage().contains("row[5]"));
	}

	@Test
	void sqlFailuresAreWrappedWithTheOperationName() throws Exception {
		PreparedStatement failing = mock(PreparedStatement.class);
		when(failing.executeQuery()).thenThrow(new SQLException("boom", "08006"));
		PostgresqlRuleRepository repository = repository(mapOf("SELECT chain_id, el_data", failing));

		RuntimeException error = assertThrows(RuntimeException.class, () -> repository.fetchChain("c1"));
		assertTrue(error.getMessage().contains("fetchChain"));
	}

	@Test
	void autoInitExecutesTheBundledDdlStatements() throws Exception {
		DataSource dataSource = mock(DataSource.class);
		Connection connection = mock(Connection.class);
		when(dataSource.getConnection()).thenReturn(connection);
		DatabaseMetaData databaseMetaData = mock(DatabaseMetaData.class);
		when(databaseMetaData.getDatabaseProductName()).thenReturn("PostgreSQL");
		when(connection.getMetaData()).thenReturn(databaseMetaData);
		Statement ddl = mock(Statement.class);
		when(ddl.execute(anyString())).thenReturn(true);
		ResultSet empty = mock(ResultSet.class);
		ResultSetMetaData columns = mock(ResultSetMetaData.class);
		when(columns.getPrecision(anyInt())).thenReturn(128);
		when(empty.getMetaData()).thenReturn(columns);
		when(ddl.executeQuery(anyString())).thenReturn(empty);
		when(connection.createStatement()).thenReturn(ddl);
		PreparedStatement latest = statement(resultSet(new Object[][] {{0L}}));
		when(connection.prepareStatement(anyString())).thenReturn(latest);

		PostgresqlRuleRepository repository = repository(dataSource, true);
		assertEquals(0, repository.fetchLatestSeq());
		// 4 tables + 1 index + 1 lock-row initialization from the bundled ddl.sql.
		verify(ddl, org.mockito.Mockito.times(6)).execute(anyString());
	}

	@Test
	void missingTablesFailFastAndCloseTheBorrowedConnection() throws Exception {
		DataSource dataSource = mock(DataSource.class);
		Connection connection = mock(Connection.class);
		when(dataSource.getConnection()).thenReturn(connection);
		DatabaseMetaData databaseMetaData = mock(DatabaseMetaData.class);
		when(databaseMetaData.getDatabaseProductName()).thenReturn("PostgreSQL");
		when(connection.getMetaData()).thenReturn(databaseMetaData);
		Statement statement = mock(Statement.class);
		when(statement.executeQuery(anyString())).thenThrow(new SQLException("relation does not exist", "42P01"));
		when(connection.createStatement()).thenReturn(statement);

		PostgresqlRuleRepository repository = repository(dataSource, false);
		assertThrows(ConfigErrorException.class, repository::fetchLatestSeq);
		verify(connection).close();
	}

	private PostgresqlRuleRepository repository(Map<String, PreparedStatement> statements) throws Exception {
		return repository(dataSource(statements), false);
	}

	private PostgresqlRuleRepository repository(DataSource dataSource, boolean autoInit) {
		PostgresqlPublisherConfig config = PostgresqlPublisherConfig.builder().applicationName("app")
				.dataSource(dataSource).tablePrefix("lf_").build();
		return new PostgresqlRuleRepository(new PostgresqlConnectionManager(config),
				new PostgresqlDialect("lf_"), "app", autoInit);
	}

	private DataSource dataSource(Map<String, PreparedStatement> statements) throws Exception {
		DataSource dataSource = mock(DataSource.class);
		Connection connection = mock(Connection.class);
		when(dataSource.getConnection()).thenReturn(connection);
		stubSchemaCheck(connection);
		when(connection.prepareStatement(anyString())).thenAnswer(invocation -> {
			String sql = invocation.getArgument(0);
			return statements.entrySet().stream()
					.filter(entry -> sql.startsWith(entry.getKey()))
					.map(Map.Entry::getValue)
					.findFirst()
					.orElseThrow(() -> new AssertionError("unexpected SQL: " + sql));
		});
		return dataSource;
	}

	private void stubSchemaCheck(Connection connection) throws Exception {
		DatabaseMetaData databaseMetaData = mock(DatabaseMetaData.class);
		when(databaseMetaData.getDatabaseProductName()).thenReturn("PostgreSQL");
		when(connection.getMetaData()).thenReturn(databaseMetaData);
		Statement statement = mock(Statement.class);
		when(connection.createStatement()).thenReturn(statement);
		ResultSet empty = mock(ResultSet.class);
		ResultSetMetaData columns = mock(ResultSetMetaData.class);
		when(columns.getPrecision(anyInt())).thenReturn(128);
		when(empty.getMetaData()).thenReturn(columns);
		when(statement.executeQuery(anyString())).thenReturn(empty);
	}

	private static Map<String, PreparedStatement> mapOf(Object... pairs) {
		Map<String, PreparedStatement> map = new LinkedHashMap<>();
		for (int i = 0; i < pairs.length; i += 2) {
			map.put((String) pairs[i], (PreparedStatement) pairs[i + 1]);
		}
		return map;
	}

	private static PreparedStatement statement(ResultSet resultSet) throws SQLException {
		PreparedStatement statement = mock(PreparedStatement.class);
		when(statement.executeQuery()).thenReturn(resultSet);
		return statement;
	}

	private static ResultSet resultSet(Object[][] rows) throws SQLException {
		ResultSet resultSet = mock(ResultSet.class);
		AtomicInteger index = new AtomicInteger(-1);
		AtomicInteger lastColumn = new AtomicInteger();
		when(resultSet.next()).thenAnswer(invocation -> index.incrementAndGet() < rows.length);
		when(resultSet.getString(anyInt())).thenAnswer(invocation -> {
			lastColumn.set(invocation.getArgument(0));
			Object value = rows[index.get()][lastColumn.get() - 1];
			return value == null ? null : value.toString();
		});
		when(resultSet.getLong(anyInt())).thenAnswer(invocation -> {
			lastColumn.set(invocation.getArgument(0));
			Object value = rows[index.get()][lastColumn.get() - 1];
			return value == null ? 0L : ((Number) value).longValue();
		});
		when(resultSet.getBoolean(anyInt())).thenAnswer(invocation -> {
			lastColumn.set(invocation.getArgument(0));
			return (Boolean) rows[index.get()][lastColumn.get() - 1];
		});
		when(resultSet.wasNull()).thenAnswer(invocation -> rows[index.get()][lastColumn.get() - 1] == null);
		return resultSet;
	}
}
