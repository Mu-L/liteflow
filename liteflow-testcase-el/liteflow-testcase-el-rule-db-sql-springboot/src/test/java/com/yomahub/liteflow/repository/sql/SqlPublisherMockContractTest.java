package com.yomahub.liteflow.repository.sql;

import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.PublishResult;
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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class SqlPublisherMockContractTest {

	@Test
	void providerValidatesConfigurationWithoutOpeningJdbc() {
		SqlRulePublisherProvider provider = new SqlRulePublisherProvider();
		DataSource dataSource = mock(DataSource.class);
		SqlPublisherConfig config = config(dataSource);

		assertTrue(provider.supports(config));
		try (RulePublisher ignored = provider.create(config)) {
			verifyNoInteractions(dataSource);
		}
		assertThrows(PublisherConfigurationException.class,
				() -> provider.create(SqlPublisherConfig.builder().applicationName("app").build()));
		assertThrows(PublisherConfigurationException.class,
				() -> provider.create(SqlPublisherConfig.builder().applicationName("app")
						.url("jdbc:mock").tablePrefix("lf_;drop").build()));
	}

	@Test
	void exactCreateCommitsGeneratedSequence() throws Exception {
		DataSource dataSource = mock(DataSource.class);
		Connection connection = connection(dataSource);
		PreparedStatement insert = mock(PreparedStatement.class);
		PreparedStatement changeLog = mock(PreparedStatement.class);
		when(connection.prepareStatement(anyString())).thenReturn(insert);
		when(connection.prepareStatement(anyString(), anyInt())).thenReturn(changeLog);
		when(insert.executeUpdate()).thenReturn(1);
		ResultSet generatedSequence = sequence(21L);
		when(changeLog.getGeneratedKeys()).thenReturn(generatedSequence);

		PublishResult result;
		try (RulePublisher publisher = publisher(dataSource)) {
			result = publisher.publishChain(chain("c1", "THEN(a)", 0L));
		}

		assertEquals(1, result.getVersion());
		assertEquals(21, result.getSequence());
		assertEquals(ChangeRecord.Op.UPSERT, result.getOperation());
		verify(connection).commit();
		verify(connection, never()).rollback();
		verify(connection).prepareStatement(anyString(), org.mockito.ArgumentMatchers.eq(Statement.RETURN_GENERATED_KEYS));
	}

	@Test
	void staleVersionRollsBackWithoutWritingChangeLog() throws Exception {
		DataSource dataSource = mock(DataSource.class);
		Connection connection = connection(dataSource);
		PreparedStatement update = mock(PreparedStatement.class);
		PreparedStatement select = mock(PreparedStatement.class);
		when(connection.prepareStatement(anyString())).thenAnswer(invocation -> {
			String sql = invocation.getArgument(0);
			return sql.startsWith("UPDATE") ? update : select;
		});
		when(update.executeUpdate()).thenReturn(0);
		ResultSet current = sequence(5L);
		when(select.executeQuery()).thenReturn(current);

		try (RulePublisher publisher = publisher(dataSource)) {
			assertThrows(VersionConflictException.class,
					() -> publisher.publishChain(chain("c1", "THEN(b)", 3L)));
		}

		verify(connection).rollback();
		verify(connection, never()).commit();
		verify(connection, never()).prepareStatement(anyString(), anyInt());
	}

	@Test
	void deleteUsesLockedVersionAndReturnsDeletePosition() throws Exception {
		DataSource dataSource = mock(DataSource.class);
		Connection connection = connection(dataSource);
		PreparedStatement select = mock(PreparedStatement.class);
		PreparedStatement delete = mock(PreparedStatement.class);
		PreparedStatement changeLog = mock(PreparedStatement.class);
		when(connection.prepareStatement(anyString())).thenAnswer(invocation -> {
			String sql = invocation.getArgument(0);
			return sql.startsWith("SELECT") ? select : delete;
		});
		when(connection.prepareStatement(anyString(), anyInt())).thenReturn(changeLog);
		ResultSet current = sequence(2L);
		ResultSet changeSequence = sequence(29L);
		when(select.executeQuery()).thenReturn(current);
		when(delete.executeUpdate()).thenReturn(1);
		when(changeLog.getGeneratedKeys()).thenReturn(changeSequence);

		PublishResult result;
		try (RulePublisher publisher = publisher(dataSource)) {
			result = publisher.removeChain(RemoveRuleRequest.builder()
					.targetId("c1").expectedVersion(2L).build());
		}

		assertEquals(ChangeRecord.Op.DELETE, result.getOperation());
		assertEquals(2, result.getVersion());
		assertEquals(29, result.getSequence());
		verify(connection).commit();
	}

	@Test
	void sqlFailureIsWrappedAndRolledBack() throws Exception {
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
	void backendLengthValidationRunsBeforeJdbc() {
		DataSource dataSource = mock(DataSource.class);
		try (RulePublisher publisher = publisher(dataSource)) {
			assertThrows(RuleValidationException.class,
					() -> publisher.publishChain(chain(repeat("c", 129), "THEN(a)", null)));
		}
		verifyNoInteractions(dataSource);
	}

	private RulePublisher publisher(DataSource dataSource) {
		return new SqlRulePublisherProvider().create(config(dataSource));
	}

	private SqlPublisherConfig config(DataSource dataSource) {
		return SqlPublisherConfig.builder().applicationName("app")
				.dataSource(dataSource).tablePrefix("lf_").build();
	}

	private Connection connection(DataSource dataSource) throws Exception {
		Connection connection = mock(Connection.class);
		when(dataSource.getConnection()).thenReturn(connection);
		when(connection.setSavepoint()).thenReturn(mock(Savepoint.class));
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
