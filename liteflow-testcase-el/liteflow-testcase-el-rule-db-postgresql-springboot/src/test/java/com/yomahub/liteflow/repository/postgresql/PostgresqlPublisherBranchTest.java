package com.yomahub.liteflow.repository.postgresql;

import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.PublishResult;
import com.yomahub.liteflow.publisher.PublishScriptRequest;
import com.yomahub.liteflow.publisher.RemoveRuleRequest;
import com.yomahub.liteflow.publisher.RulePublisher;
import com.yomahub.liteflow.publisher.exception.VersionConflictException;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Branch coverage for the transactional publisher beyond the contract test. */
class PostgresqlPublisherBranchTest {

	@Test
	void unversionedUpdateReturnsTheCurrentVersionAfterIncrement() throws Exception {
		DataSource dataSource = mock(DataSource.class);
		Connection connection = connection(dataSource);
		PreparedStatement update = mock(PreparedStatement.class);
		PreparedStatement select = mock(PreparedStatement.class);
		PreparedStatement changeLog = mock(PreparedStatement.class);
		when(connection.prepareStatement(anyString())).thenAnswer(invocation -> {
			String sql = invocation.getArgument(0);
			if (sql.startsWith("UPDATE")) { return update; }
			if (sql.startsWith("SELECT")) { return select; }
			return changeLog;
		});
		when(update.executeUpdate()).thenReturn(1);
		ResultSet current = rows(new Object[][] {{5L}});
		when(select.executeQuery()).thenReturn(current);
		ResultSet changeSequence = rows(new Object[][] {{11L}});
		when(changeLog.executeQuery()).thenReturn(changeSequence);

		PublishResult result;
		try (RulePublisher publisher = publisher(dataSource)) {
			result = publisher.publishChain(PublishChainRequest.builder().chainId("c1").el("THEN(a)").build());
		}

		assertEquals(5, result.getVersion());
		assertEquals(11, result.getSequence());
		verify(connection).commit();
	}

	@Test
	void insertRaceFallsBackToUpdateBehindASavepoint() throws Exception {
		DataSource dataSource = mock(DataSource.class);
		Connection connection = connection(dataSource);
		PreparedStatement update = mock(PreparedStatement.class);
		PreparedStatement select = mock(PreparedStatement.class);
		PreparedStatement insert = mock(PreparedStatement.class);
		PreparedStatement changeLog = mock(PreparedStatement.class);
		when(connection.prepareStatement(anyString())).thenAnswer(invocation -> {
			String sql = invocation.getArgument(0);
			if (sql.startsWith("UPDATE")) { return update; }
			if (sql.startsWith("SELECT")) { return select; }
			if (sql.contains("change_log")) { return changeLog; }
			return insert;
		});
		// First update misses, insert loses the race, retry update wins.
		when(update.executeUpdate()).thenReturn(0, 1);
		when(insert.executeUpdate()).thenThrow(new SQLException("duplicate key", "23505"));
		ResultSet beforeRace = rows(new Object[0][]);
		ResultSet afterRace = rows(new Object[][] {{3L}});
		when(select.executeQuery()).thenReturn(beforeRace, afterRace);
		ResultSet raceSequence = rows(new Object[][] {{21L}});
		when(changeLog.executeQuery()).thenReturn(raceSequence);

		PublishResult result;
		try (RulePublisher publisher = publisher(dataSource)) {
			result = publisher.publishChain(PublishChainRequest.builder().chainId("c1").el("THEN(a)").build());
		}

		assertEquals(3, result.getVersion());
		assertEquals(21, result.getSequence());
		verify(connection).commit();
	}

	@Test
	void exactCreateOnExistingChainConflictsAfterSavepointRollback() throws Exception {
		DataSource dataSource = mock(DataSource.class);
		Connection connection = connection(dataSource);
		PreparedStatement insert = mock(PreparedStatement.class);
		PreparedStatement select = mock(PreparedStatement.class);
		when(connection.prepareStatement(anyString())).thenAnswer(invocation -> {
			String sql = invocation.getArgument(0);
			return sql.startsWith("SELECT") ? select : insert;
		});
		when(insert.executeUpdate()).thenThrow(new SQLException("duplicate key", "23505"));
		ResultSet existing = rows(new Object[][] {{4L}});
		when(select.executeQuery()).thenReturn(existing);

		try (RulePublisher publisher = publisher(dataSource)) {
			assertThrows(VersionConflictException.class, () -> publisher.publishChain(
					PublishChainRequest.builder().chainId("c1").el("THEN(a)").expectedVersion(0L).build()));
		}
		verify(connection).rollback();
	}

	@Test
	void publishScriptSupportsExactCreateAndVersionedUpdate() throws Exception {
		DataSource dataSource = mock(DataSource.class);
		Connection connection = connection(dataSource);
		PreparedStatement insert = mock(PreparedStatement.class);
		PreparedStatement update = mock(PreparedStatement.class);
		PreparedStatement changeLog = mock(PreparedStatement.class);
		when(connection.prepareStatement(anyString())).thenAnswer(invocation -> {
			String sql = invocation.getArgument(0);
			if (sql.startsWith("UPDATE")) { return update; }
			if (sql.contains("change_log")) { return changeLog; }
			return insert;
		});
		when(insert.executeUpdate()).thenReturn(1);
		when(update.executeUpdate()).thenReturn(1);
		AtomicInteger seq = new AtomicInteger();
		when(changeLog.executeQuery()).thenAnswer(invocation -> rows(new Object[][] {{(long) seq.incrementAndGet()}}));

		try (RulePublisher publisher = publisher(dataSource)) {
			PublishResult created = publisher.publishScript(PublishScriptRequest.builder()
					.nodeId("s1").script("return 1").type("script").language("groovy").expectedVersion(0L).build());
			assertEquals(1, created.getVersion());

			PublishResult updated = publisher.publishScript(PublishScriptRequest.builder()
					.nodeId("s1").script("return 2").type("script").language("groovy").expectedVersion(1L).build());
			assertEquals(2, updated.getVersion());
		}
		verify(connection, org.mockito.Mockito.times(2)).commit();
	}

	@Test
	void removeWithZeroExpectedVersionOnExistingRowConflicts() throws Exception {
		DataSource dataSource = mock(DataSource.class);
		Connection connection = connection(dataSource);
		PreparedStatement select = mock(PreparedStatement.class);
		when(connection.prepareStatement(anyString())).thenReturn(select);
		ResultSet locked = rows(new Object[][] {{2L}});
		when(select.executeQuery()).thenReturn(locked);

		try (RulePublisher publisher = publisher(dataSource)) {
			assertThrows(VersionConflictException.class, () -> publisher.removeChain(
					RemoveRuleRequest.builder().targetId("c1").expectedVersion(0L).build()));
		}
		verify(connection).rollback();
	}

	@Test
	void removeScriptDeletesTheLockedRow() throws Exception {
		DataSource dataSource = mock(DataSource.class);
		Connection connection = connection(dataSource);
		PreparedStatement select = mock(PreparedStatement.class);
		PreparedStatement delete = mock(PreparedStatement.class);
		PreparedStatement changeLog = mock(PreparedStatement.class);
		when(connection.prepareStatement(anyString())).thenAnswer(invocation -> {
			String sql = invocation.getArgument(0);
			if (sql.startsWith("SELECT")) { return select; }
			if (sql.startsWith("DELETE")) { return delete; }
			return changeLog;
		});
		ResultSet locked = rows(new Object[][] {{3L}});
		when(select.executeQuery()).thenReturn(locked);
		when(delete.executeUpdate()).thenReturn(1);
		ResultSet changeSequence = rows(new Object[][] {{30L}});
		when(changeLog.executeQuery()).thenReturn(changeSequence);

		PublishResult result;
		try (RulePublisher publisher = publisher(dataSource)) {
			result = publisher.removeScript(RemoveRuleRequest.builder().targetId("s1").expectedVersion(3L).build());
		}

		assertEquals(ChangeRecord.TargetType.SCRIPT, result.getTargetType());
		assertEquals(ChangeRecord.Op.DELETE, result.getOperation());
		assertEquals(3, result.getVersion());
		assertEquals(30, result.getSequence());
		verify(connection).commit();
	}

	@Test
	void deleteThatLosesTheRowMidTransactionConflicts() throws Exception {
		DataSource dataSource = mock(DataSource.class);
		Connection connection = connection(dataSource);
		PreparedStatement select = mock(PreparedStatement.class);
		PreparedStatement delete = mock(PreparedStatement.class);
		when(connection.prepareStatement(anyString())).thenAnswer(invocation -> {
			String sql = invocation.getArgument(0);
			return sql.startsWith("SELECT") ? select : delete;
		});
		ResultSet vanished = rows(new Object[][] {{2L}});
		when(select.executeQuery()).thenReturn(vanished);
		when(delete.executeUpdate()).thenReturn(0);

		try (RulePublisher publisher = publisher(dataSource)) {
			assertThrows(VersionConflictException.class, () -> publisher.removeChain(
					RemoveRuleRequest.builder().targetId("c1").build()));
		}
		verify(connection).rollback();
	}

	private Connection connection(DataSource dataSource) throws Exception {
		Connection connection = mock(Connection.class);
		when(dataSource.getConnection()).thenReturn(connection);
		when(connection.setSavepoint()).thenReturn(mock(Savepoint.class));
		Statement lockStatement = mock(Statement.class);
		ResultSet lockRow = mock(ResultSet.class);
		when(lockRow.next()).thenReturn(true);
		when(lockStatement.executeQuery(anyString())).thenReturn(lockRow);
		when(connection.createStatement()).thenReturn(lockStatement);
		return connection;
	}

	private RulePublisher publisher(DataSource dataSource) {
		return new PostgresqlRulePublisherProvider().create(PostgresqlPublisherConfig.builder()
				.applicationName("app").dataSource(dataSource).tablePrefix("lf_").build());
	}

	private static ResultSet rows(Object[][] data) throws SQLException {
		ResultSet resultSet = mock(ResultSet.class);
		AtomicInteger index = new AtomicInteger(-1);
		when(resultSet.next()).thenAnswer(invocation -> index.incrementAndGet() < data.length);
		when(resultSet.getLong(org.mockito.ArgumentMatchers.anyInt())).thenAnswer(invocation ->
				((Number) data[index.get()][(Integer) invocation.getArgument(0) - 1]).longValue());
		return resultSet;
	}
}
