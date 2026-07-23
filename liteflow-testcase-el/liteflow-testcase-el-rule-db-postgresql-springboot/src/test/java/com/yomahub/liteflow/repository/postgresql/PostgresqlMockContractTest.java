package com.yomahub.liteflow.repository.postgresql;

import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.PublishResult;
import com.yomahub.liteflow.publisher.PublishScriptRequest;
import com.yomahub.liteflow.publisher.RemoveRuleRequest;
import com.yomahub.liteflow.publisher.RulePublisher;
import com.yomahub.liteflow.publisher.exception.PublisherConfigurationException;
import com.yomahub.liteflow.publisher.exception.RuleStorageException;
import com.yomahub.liteflow.publisher.exception.RuleValidationException;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class PostgresqlMockContractTest {

	@Test
	void providerValidatesConfigurationWithoutOpeningAConnection() throws Exception {
		PostgresqlRulePublisherProvider provider = new PostgresqlRulePublisherProvider();
		DataSource dataSource = mock(DataSource.class);
		PostgresqlPublisherConfig config = config(dataSource);

		assertTrue(provider.supports(config));
		try (RulePublisher ignored = provider.create(config)) {
			verifyNoInteractions(dataSource);
		}
		assertThrows(PublisherConfigurationException.class,
				() -> provider.create(PostgresqlPublisherConfig.builder().applicationName("app").build()));
		assertThrows(PublisherConfigurationException.class,
				() -> provider.create(PostgresqlPublisherConfig.builder().applicationName("app")
						.url("jdbc:postgresql://mock.invalid/db").tablePrefix("lf_;drop").build()));
	}

	@Test
	void exactCreateCommitsRuleAndChangeLogInOneTransaction() throws Exception {
		DataSource dataSource = mock(DataSource.class);
		Connection connection = connection(dataSource);
		PreparedStatement insert = mock(PreparedStatement.class);
		PreparedStatement changeLog = mock(PreparedStatement.class);
		ResultSet sequence = sequence(41L);
		when(connection.prepareStatement(anyString())).thenAnswer(invocation -> {
			String sql = invocation.getArgument(0);
			return sql.contains("change_log") ? changeLog : insert;
		});
		when(insert.executeUpdate()).thenReturn(1);
		when(changeLog.executeQuery()).thenReturn(sequence);

		PublishResult result;
		try (RulePublisher publisher = publisher(dataSource)) {
			result = publisher.publishChain(chain("c1", "THEN(a)", 0L));
		}

		assertEquals(1, result.getVersion());
		assertEquals(41, result.getSequence());
		assertEquals(ChangeRecord.TargetType.CHAIN, result.getTargetType());
		verify(connection).setAutoCommit(false);
		verify(connection).commit();
		verify(connection, never()).rollback();
	}

	@Test
	void exactUpdateReturnsNextVersion() throws Exception {
		DataSource dataSource = mock(DataSource.class);
		Connection connection = connection(dataSource);
		PreparedStatement update = mock(PreparedStatement.class);
		PreparedStatement changeLog = mock(PreparedStatement.class);
		when(connection.prepareStatement(anyString())).thenAnswer(invocation -> {
			String sql = invocation.getArgument(0);
			return sql.startsWith("UPDATE") ? update : changeLog;
		});
		when(update.executeUpdate()).thenReturn(1);
		ResultSet changeSequence = sequence(9L);
		when(changeLog.executeQuery()).thenReturn(changeSequence);

		PublishResult result;
		try (RulePublisher publisher = publisher(dataSource)) {
			result = publisher.publishChain(chain("c1", "THEN(b)", 3L));
		}

		assertEquals(4, result.getVersion());
		assertEquals(9, result.getSequence());
		verify(update).setLong(7, 3L);
		verify(connection).commit();
	}

	@Test
	void staleVersionRollsBackWithoutAppendingAChange() throws Exception {
		DataSource dataSource = mock(DataSource.class);
		Connection connection = connection(dataSource);
		PreparedStatement update = mock(PreparedStatement.class);
		PreparedStatement select = mock(PreparedStatement.class);
		ResultSet current = sequence(5L);
		when(connection.prepareStatement(anyString())).thenAnswer(invocation -> {
			String sql = invocation.getArgument(0);
			if (sql.startsWith("UPDATE")) { return update; }
			if (sql.startsWith("SELECT")) { return select; }
			throw new AssertionError("unexpected SQL: " + sql);
		});
		when(update.executeUpdate()).thenReturn(0);
		when(select.executeQuery()).thenReturn(current);

		try (RulePublisher publisher = publisher(dataSource)) {
			assertThrows(VersionConflictException.class,
					() -> publisher.publishChain(chain("c1", "THEN(b)", 3L)));
		}

		verify(connection).rollback();
		verify(connection, never()).commit();
	}

	@Test
	void deleteUsesLockedVersionAndAppendsDeleteChange() throws Exception {
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
		ResultSet currentVersion = sequence(2L);
		ResultSet changeSequence = sequence(12L);
		when(select.executeQuery()).thenReturn(currentVersion);
		when(delete.executeUpdate()).thenReturn(1);
		when(changeLog.executeQuery()).thenReturn(changeSequence);

		PublishResult result;
		try (RulePublisher publisher = publisher(dataSource)) {
			result = publisher.removeChain(RemoveRuleRequest.builder()
					.targetId("c1").expectedVersion(2L).build());
		}

		assertEquals(ChangeRecord.Op.DELETE, result.getOperation());
		assertEquals(2, result.getVersion());
		assertEquals(12, result.getSequence());
		verify(delete).setLong(3, 2L);
		verify(connection).commit();
	}

	@Test
	void jdbcFailureIsWrappedAndRolledBack() throws Exception {
		DataSource dataSource = mock(DataSource.class);
		Connection connection = connection(dataSource);
		PreparedStatement insert = mock(PreparedStatement.class);
		when(connection.prepareStatement(anyString())).thenReturn(insert);
		when(insert.executeUpdate()).thenThrow(new SQLException("database unavailable", "08006"));

		try (RulePublisher publisher = publisher(dataSource)) {
			RuleStorageException error = assertThrows(RuleStorageException.class,
					() -> publisher.publishChain(chain("c1", "THEN(a)", 0L)));
			assertTrue(error.getMessage().contains("database unavailable"));
		}
		verify(connection).rollback();
	}

	@Test
	void backendLengthValidationRunsBeforeJdbc() throws Exception {
		DataSource dataSource = mock(DataSource.class);
		try (RulePublisher publisher = publisher(dataSource)) {
			assertThrows(RuleValidationException.class,
					() -> publisher.publishChain(chain(repeat("c", 129), "THEN(a)", null)));
			assertThrows(RuleValidationException.class,
					() -> publisher.publishScript(PublishScriptRequest.builder().nodeId("s")
							.script("return true").type("script").language(repeat("l", 33)).build()));
		}
		verifyNoInteractions(dataSource);
	}

	private RulePublisher publisher(DataSource dataSource) {
		return new PostgresqlRulePublisherProvider().create(config(dataSource));
	}

	private PostgresqlPublisherConfig config(DataSource dataSource) {
		return PostgresqlPublisherConfig.builder().applicationName("app")
				.dataSource(dataSource).tablePrefix("lf_").build();
	}

	private Connection connection(DataSource dataSource) throws Exception {
		Connection connection = mock(Connection.class);
		when(dataSource.getConnection()).thenReturn(connection);
		when(connection.setSavepoint()).thenReturn(mock(Savepoint.class));
		Statement lock = mock(Statement.class);
		ResultSet lockRow = sequence(1L);
		when(connection.createStatement()).thenReturn(lock);
		when(lock.executeQuery(anyString())).thenReturn(lockRow);
		return connection;
	}

	private ResultSet sequence(long value) throws Exception {
		ResultSet resultSet = mock(ResultSet.class);
		when(resultSet.next()).thenReturn(true);
		when(resultSet.getLong(1)).thenReturn(value);
		return resultSet;
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
