package com.yomahub.liteflow.repository.postgresql;

import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.RulePublisher;
import com.yomahub.liteflow.repository.vo.RuleManifest;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Borrowed connections must be returned to the pool with their state restored. */
class PostgresqlConnectionResetTest {

	@Test
	void fetchManifestRestoresIsolationAndAutoCommitBeforeReturningConnection() throws Exception {
		DataSource dataSource = mock(DataSource.class);
		Connection connection = mock(Connection.class);
		when(dataSource.getConnection()).thenReturn(connection);
		when(connection.getAutoCommit()).thenReturn(true);
		when(connection.getTransactionIsolation()).thenReturn(Connection.TRANSACTION_READ_COMMITTED);
		stubSchemaCheck(connection);
		stubManifestQueries(connection);

		PostgresqlRuleRepository repository = repository(dataSource);
		RuleManifest manifest = repository.fetchManifest();

		assertEquals(1, manifest.getChains().size());
		assertEquals(42, manifest.getLatestSeq());
		InOrder inOrder = inOrder(connection);
		inOrder.verify(connection).setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
		inOrder.verify(connection).setAutoCommit(false);
		inOrder.verify(connection).commit();
		inOrder.verify(connection).setAutoCommit(true);
		inOrder.verify(connection).setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
		inOrder.verify(connection).close();
	}

	@Test
	void fetchManifestRestoresConnectionStateEvenWhenTheQueryFails() throws Exception {
		DataSource dataSource = mock(DataSource.class);
		Connection connection = mock(Connection.class);
		when(dataSource.getConnection()).thenReturn(connection);
		when(connection.getAutoCommit()).thenReturn(true);
		when(connection.getTransactionIsolation()).thenReturn(Connection.TRANSACTION_READ_COMMITTED);
		stubSchemaCheck(connection);
		PreparedStatement failing = mock(PreparedStatement.class);
		when(failing.executeQuery()).thenThrow(new SQLException("database unavailable", "08006"));
		when(connection.prepareStatement(anyString())).thenReturn(failing);

		PostgresqlRuleRepository repository = repository(dataSource);
		assertThrows(RuntimeException.class, repository::fetchManifest);

		verify(connection).setAutoCommit(true);
		verify(connection).setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
		verify(connection).close();
	}

	@Test
	void publisherRestoresAutoCommitAfterACommit() throws Exception {
		DataSource dataSource = mock(DataSource.class);
		Connection connection = mock(Connection.class);
		when(dataSource.getConnection()).thenReturn(connection);
		when(connection.getAutoCommit()).thenReturn(true);
		when(connection.setSavepoint()).thenReturn(mock(Savepoint.class));
		stubPublishingLock(connection);
		PreparedStatement insert = mock(PreparedStatement.class);
		PreparedStatement changeLog = mock(PreparedStatement.class);
		when(connection.prepareStatement(anyString())).thenAnswer(invocation -> {
			String sql = invocation.getArgument(0);
			return sql.contains("change_log") ? changeLog : insert;
		});
		when(insert.executeUpdate()).thenReturn(1);
		ResultSet sequence = mock(ResultSet.class);
		when(sequence.next()).thenReturn(true);
		when(sequence.getLong(1)).thenReturn(7L);
		when(changeLog.executeQuery()).thenReturn(sequence);

		try (RulePublisher publisher = publisher(dataSource)) {
			publisher.publishChain(PublishChainRequest.builder().chainId("c1").el("THEN(a)")
					.expectedVersion(0L).build());
		}

		InOrder inOrder = inOrder(connection);
		inOrder.verify(connection).setAutoCommit(false);
		inOrder.verify(connection).commit();
		inOrder.verify(connection).setAutoCommit(true);
		inOrder.verify(connection).close();
	}

	@Test
	void publisherRestoresAutoCommitAfterARollback() throws Exception {
		DataSource dataSource = mock(DataSource.class);
		Connection connection = mock(Connection.class);
		when(dataSource.getConnection()).thenReturn(connection);
		when(connection.getAutoCommit()).thenReturn(true);
		when(connection.setSavepoint()).thenReturn(mock(Savepoint.class));
		stubPublishingLock(connection);
		PreparedStatement failing = mock(PreparedStatement.class);
		when(connection.prepareStatement(anyString())).thenReturn(failing);
		when(failing.executeUpdate()).thenThrow(new SQLException("database unavailable", "08006"));

		try (RulePublisher publisher = publisher(dataSource)) {
			assertThrows(RuntimeException.class, () -> publisher.publishChain(
					PublishChainRequest.builder().chainId("c1").el("THEN(a)").expectedVersion(0L).build()));
		}

		InOrder inOrder = inOrder(connection);
		inOrder.verify(connection).rollback();
		inOrder.verify(connection).setAutoCommit(true);
		inOrder.verify(connection).close();
	}

	private PostgresqlRuleRepository repository(DataSource dataSource) {
		PostgresqlPublisherConfig config = PostgresqlPublisherConfig.builder().applicationName("app")
				.dataSource(dataSource).tablePrefix("lf_").build();
		return new PostgresqlRuleRepository(new PostgresqlConnectionManager(config),
				new PostgresqlDialect("lf_"), "app", false);
	}

	private RulePublisher publisher(DataSource dataSource) {
		return new PostgresqlRulePublisherProvider().create(PostgresqlPublisherConfig.builder()
				.applicationName("app").dataSource(dataSource).tablePrefix("lf_").build());
	}

	private void stubManifestQueries(Connection connection) throws Exception {
		PreparedStatement chain = mock(PreparedStatement.class);
		PreparedStatement script = mock(PreparedStatement.class);
		PreparedStatement latestSeq = mock(PreparedStatement.class);
		when(connection.prepareStatement(anyString())).thenAnswer(invocation -> {
			String sql = invocation.getArgument(0);
			if (sql.startsWith("SELECT chain_id")) { return chain; }
			if (sql.startsWith("SELECT node_id")) { return script; }
			return latestSeq;
		});
		ResultSet chainRows = mock(ResultSet.class);
		when(chainRows.next()).thenReturn(true, false);
		when(chainRows.getString(1)).thenReturn("c1");
		when(chainRows.getLong(2)).thenReturn(3L);
		when(chainRows.getString(3)).thenReturn("md5");
		when(chain.executeQuery()).thenReturn(chainRows);
		ResultSet scriptRows = mock(ResultSet.class);
		when(scriptRows.next()).thenReturn(false);
		when(script.executeQuery()).thenReturn(scriptRows);
		ResultSet seqRow = mock(ResultSet.class);
		when(seqRow.next()).thenReturn(true);
		when(seqRow.getLong(1)).thenReturn(42L);
		when(seqRow.wasNull()).thenReturn(false);
		when(latestSeq.executeQuery()).thenReturn(seqRow);
	}

	private void stubPublishingLock(Connection connection) throws Exception {
		Statement statement = mock(Statement.class);
		ResultSet row = mock(ResultSet.class);
		when(connection.createStatement()).thenReturn(statement);
		when(statement.executeQuery(anyString())).thenReturn(row);
		when(row.next()).thenReturn(true);
	}

	private void stubSchemaCheck(Connection connection) throws Exception {
		DatabaseMetaData databaseMetaData = mock(DatabaseMetaData.class);
		when(databaseMetaData.getDatabaseProductName()).thenReturn("PostgreSQL");
		when(connection.getMetaData()).thenReturn(databaseMetaData);
		Statement statement = mock(Statement.class);
		when(connection.createStatement()).thenReturn(statement);
		ResultSet empty = mock(ResultSet.class);
		ResultSetMetaData columns = mock(ResultSetMetaData.class);
		when(columns.getPrecision(org.mockito.ArgumentMatchers.anyInt())).thenReturn(128);
		when(empty.getMetaData()).thenReturn(columns);
		when(statement.executeQuery(anyString())).thenReturn(empty);
	}
}
