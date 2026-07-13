package com.yomahub.liteflow.repository.sql;

import cn.hutool.crypto.SecureUtil;
import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.PublishResult;
import com.yomahub.liteflow.publisher.PublishScriptRequest;
import com.yomahub.liteflow.publisher.RemoveRuleRequest;
import com.yomahub.liteflow.publisher.RulePublisher;
import com.yomahub.liteflow.publisher.exception.RuleStorageException;
import com.yomahub.liteflow.publisher.exception.VersionConflictException;
import com.yomahub.liteflow.repository.vo.ChangeRecord;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/** SQL transaction implementation of the backend-neutral publisher API. */
final class SqlRulePublisherImpl implements RulePublisher {

	private final SqlConnectionManager connectionManager;
	private final SqlDialect dialect;
	private final String applicationName;

	SqlRulePublisherImpl(SqlPublisherConfig config) {
		this.connectionManager = new SqlConnectionManager(config);
		this.dialect = new SqlDialect(config.getTablePrefix());
		this.applicationName = config.applicationName();
	}

	@Override
	public PublishResult publishChain(PublishChainRequest request) {
		return inTransaction("publish chain", connection -> {
			long version = upsertChain(connection, request);
			long sequence = insertChangeLog(connection, ChangeRecord.TargetType.CHAIN,
					request.getChainId(), ChangeRecord.Op.UPSERT, version);
			return result(request.getChainId(), ChangeRecord.TargetType.CHAIN,
					ChangeRecord.Op.UPSERT, version, sequence);
		});
	}

	@Override
	public PublishResult publishScript(PublishScriptRequest request) {
		return inTransaction("publish script", connection -> {
			long version = upsertScript(connection, request);
			long sequence = insertChangeLog(connection, ChangeRecord.TargetType.SCRIPT,
					request.getNodeId(), ChangeRecord.Op.UPSERT, version);
			return result(request.getNodeId(), ChangeRecord.TargetType.SCRIPT,
					ChangeRecord.Op.UPSERT, version, sequence);
		});
	}

	@Override
	public PublishResult removeChain(RemoveRuleRequest request) {
		return remove(request, ChangeRecord.TargetType.CHAIN, dialect.chainTable(), "chain_id");
	}

	@Override
	public PublishResult removeScript(RemoveRuleRequest request) {
		return remove(request, ChangeRecord.TargetType.SCRIPT, dialect.scriptTable(), "node_id");
	}

	@Override
	public void close() {
		// Connections are borrowed per operation; the configured DataSource remains caller-owned.
	}

	private long upsertChain(Connection connection, PublishChainRequest request) throws SQLException {
		Long expected = request.getExpectedVersion();
		if (expected != null && expected == 0) {
			try {
				insertChain(connection, request);
				return 1;
			}
			catch (SQLException e) {
				if (isConstraintViolation(e)) {
					throw conflict("chain", request.getChainId(), expected,
							currentVersion(connection, dialect.chainTable(), "chain_id", request.getChainId()));
				}
				throw e;
			}
		}

		StringBuilder sql = new StringBuilder("UPDATE ").append(dialect.chainTable())
				.append(" SET el_data = ?, route_data = ?, namespace = ?, content_md5 = ?, ")
				.append("version = version + 1, enable = 1, gmt_modified = CURRENT_TIMESTAMP")
				.append(" WHERE application_name = ? AND chain_id = ?");
		if (expected != null) {
			sql.append(" AND version = ?");
		}
		int updated;
		try (PreparedStatement statement = connection.prepareStatement(sql.toString())) {
			statement.setString(1, request.getEl());
			statement.setString(2, request.getRoute());
			statement.setString(3, request.getNamespace());
			statement.setString(4, SecureUtil.md5(request.getEl()));
			statement.setString(5, applicationName);
			statement.setString(6, request.getChainId());
			if (expected != null) {
				statement.setLong(7, expected);
			}
			updated = statement.executeUpdate();
		}
		if (updated == 0) {
			long current = currentVersion(connection, dialect.chainTable(), "chain_id", request.getChainId());
			if (expected != null) {
				throw conflict("chain", request.getChainId(), expected, current);
			}
			insertChain(connection, request);
			return 1;
		}
		return expected == null
				? currentVersion(connection, dialect.chainTable(), "chain_id", request.getChainId())
				: expected + 1;
	}

	private void insertChain(Connection connection, PublishChainRequest request) throws SQLException {
		String sql = "INSERT INTO " + dialect.chainTable()
				+ " (application_name, chain_id, namespace, el_data, route_data, content_md5, version, enable)"
				+ " VALUES (?, ?, ?, ?, ?, ?, 1, 1)";
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setString(1, applicationName);
			statement.setString(2, request.getChainId());
			statement.setString(3, request.getNamespace());
			statement.setString(4, request.getEl());
			statement.setString(5, request.getRoute());
			statement.setString(6, SecureUtil.md5(request.getEl()));
			statement.executeUpdate();
		}
	}

	private long upsertScript(Connection connection, PublishScriptRequest request) throws SQLException {
		Long expected = request.getExpectedVersion();
		if (expected != null && expected == 0) {
			try {
				insertScript(connection, request);
				return 1;
			}
			catch (SQLException e) {
				if (isConstraintViolation(e)) {
					throw conflict("script", request.getNodeId(), expected,
							currentVersion(connection, dialect.scriptTable(), "node_id", request.getNodeId()));
				}
				throw e;
			}
		}

		StringBuilder sql = new StringBuilder("UPDATE ").append(dialect.scriptTable())
				.append(" SET script_data = ?, script_name = ?, script_type = ?, script_language = ?, ")
				.append("content_md5 = ?, version = version + 1, enable = 1, gmt_modified = CURRENT_TIMESTAMP")
				.append(" WHERE application_name = ? AND node_id = ?");
		if (expected != null) {
			sql.append(" AND version = ?");
		}
		int updated;
		try (PreparedStatement statement = connection.prepareStatement(sql.toString())) {
			statement.setString(1, request.getScript());
			statement.setString(2, request.getName());
			statement.setString(3, request.getType());
			statement.setString(4, request.getLanguage());
			statement.setString(5, SecureUtil.md5(request.getScript()));
			statement.setString(6, applicationName);
			statement.setString(7, request.getNodeId());
			if (expected != null) {
				statement.setLong(8, expected);
			}
			updated = statement.executeUpdate();
		}
		if (updated == 0) {
			long current = currentVersion(connection, dialect.scriptTable(), "node_id", request.getNodeId());
			if (expected != null) {
				throw conflict("script", request.getNodeId(), expected, current);
			}
			insertScript(connection, request);
			return 1;
		}
		return expected == null
				? currentVersion(connection, dialect.scriptTable(), "node_id", request.getNodeId())
				: expected + 1;
	}

	private void insertScript(Connection connection, PublishScriptRequest request) throws SQLException {
		String sql = "INSERT INTO " + dialect.scriptTable()
				+ " (application_name, node_id, script_data, script_name, script_type, script_language, "
				+ "content_md5, version, enable) VALUES (?, ?, ?, ?, ?, ?, ?, 1, 1)";
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setString(1, applicationName);
			statement.setString(2, request.getNodeId());
			statement.setString(3, request.getScript());
			statement.setString(4, request.getName());
			statement.setString(5, request.getType());
			statement.setString(6, request.getLanguage());
			statement.setString(7, SecureUtil.md5(request.getScript()));
			statement.executeUpdate();
		}
	}

	private PublishResult remove(RemoveRuleRequest request, ChangeRecord.TargetType targetType,
			String table, String idColumn) {
		return inTransaction("remove " + targetType.name().toLowerCase(), connection -> {
			long current = lockedVersion(connection, table, idColumn, request.getTargetId());
			Long expected = request.getExpectedVersion();
			if (expected != null && (expected == 0 || expected != current)) {
				throw conflict(targetType.name().toLowerCase(), request.getTargetId(), expected, current);
			}
			if (current > 0) {
				String sql = "DELETE FROM " + table + " WHERE application_name = ? AND " + idColumn
						+ " = ? AND version = ?";
				try (PreparedStatement statement = connection.prepareStatement(sql)) {
					statement.setString(1, applicationName);
					statement.setString(2, request.getTargetId());
					statement.setLong(3, current);
					if (statement.executeUpdate() != 1) {
						throw conflict(targetType.name().toLowerCase(), request.getTargetId(), expected, current);
					}
				}
			}
			long sequence = insertChangeLog(connection, targetType,
					request.getTargetId(), ChangeRecord.Op.DELETE, current);
			return result(request.getTargetId(), targetType, ChangeRecord.Op.DELETE, current, sequence);
		});
	}

	private long currentVersion(Connection connection, String table, String idColumn, String targetId)
			throws SQLException {
		return selectVersion(connection, table, idColumn, targetId, false);
	}

	private long lockedVersion(Connection connection, String table, String idColumn, String targetId)
			throws SQLException {
		return selectVersion(connection, table, idColumn, targetId, true);
	}

	private long selectVersion(Connection connection, String table, String idColumn, String targetId,
			boolean forUpdate) throws SQLException {
		String sql = "SELECT version FROM " + table + " WHERE application_name = ? AND " + idColumn + " = ?"
				+ (forUpdate ? " FOR UPDATE" : "");
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setString(1, applicationName);
			statement.setString(2, targetId);
			try (ResultSet resultSet = statement.executeQuery()) {
				return resultSet.next() ? resultSet.getLong(1) : 0;
			}
		}
	}

	private long insertChangeLog(Connection connection, ChangeRecord.TargetType targetType,
			String targetId, ChangeRecord.Op operation, long version) throws SQLException {
		String sql = "INSERT INTO " + dialect.changeLogTable()
				+ " (application_name, target_type, target_id, op, version) VALUES (?, ?, ?, ?, ?)";
		try (PreparedStatement statement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
			statement.setString(1, applicationName);
			statement.setString(2, targetType.name());
			statement.setString(3, targetId);
			statement.setString(4, operation.name());
			statement.setLong(5, version);
			statement.executeUpdate();
			try (ResultSet keys = statement.getGeneratedKeys()) {
				if (keys.next()) {
					return keys.getLong(1);
				}
			}
		}
		throw new SQLException("change log insert did not return a generated sequence");
	}

	private PublishResult result(String targetId, ChangeRecord.TargetType targetType,
			ChangeRecord.Op operation, long version, long sequence) {
		return PublishResult.builder()
				.targetId(targetId)
				.targetType(targetType)
				.operation(operation)
				.version(version)
				.sequence(sequence)
				.build();
	}

	private VersionConflictException conflict(String targetType, String targetId, long expected, long current) {
		return new VersionConflictException(targetType + "[" + targetId + "] expected version["
				+ expected + "] but current version is[" + current + "]");
	}

	private boolean isConstraintViolation(SQLException e) {
		return e.getSQLState() != null && e.getSQLState().startsWith("23");
	}

	private PublishResult inTransaction(String operation, SqlWork work) {
		try (Connection connection = connectionManager.getConnection()) {
			connection.setAutoCommit(false);
			try {
				PublishResult result = work.execute(connection);
				connection.commit();
				return result;
			}
			catch (RuntimeException | SQLException e) {
				rollbackQuietly(connection);
				if (e instanceof RuntimeException) {
					throw (RuntimeException) e;
				}
				throw new RuleStorageException("SQL " + operation + " failed: " + e.getMessage(), e);
			}
		}
		catch (SQLException e) {
			throw new RuleStorageException("SQL " + operation + " failed: " + e.getMessage(), e);
		}
	}

	private void rollbackQuietly(Connection connection) {
		try {
			connection.rollback();
		}
		catch (SQLException ignored) {
		}
	}

	@FunctionalInterface
	private interface SqlWork {

		PublishResult execute(Connection connection) throws SQLException;
	}
}
