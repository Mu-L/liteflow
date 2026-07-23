package com.yomahub.liteflow.repository.postgresql;

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
import java.sql.Savepoint;

/** Transactional PostgreSQL implementation of the unified publisher API. */
final class PostgresqlRulePublisher implements RulePublisher {

	private final PostgresqlConnectionManager connectionManager;
	private final PostgresqlDialect dialect;
	private final String applicationName;

	PostgresqlRulePublisher(PostgresqlPublisherConfig config) {
		this.connectionManager = new PostgresqlConnectionManager(config);
		this.dialect = new PostgresqlDialect(config.getTablePrefix());
		this.applicationName = config.applicationName();
	}

	@Override
	public PublishResult publishChain(PublishChainRequest request) {
		PostgresqlStorageValidator.validateChainRequest(request);
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
		PostgresqlStorageValidator.validateScriptRequest(request);
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
		PostgresqlStorageValidator.validateRemoveRequest(request);
		return remove(request, ChangeRecord.TargetType.CHAIN, dialect.chainTable(), "chain_id");
	}

	@Override
	public PublishResult removeScript(RemoveRuleRequest request) {
		PostgresqlStorageValidator.validateRemoveRequest(request);
		return remove(request, ChangeRecord.TargetType.SCRIPT, dialect.scriptTable(), "node_id");
	}

	@Override public void close() { }

	private long upsertChain(Connection connection, PublishChainRequest request) throws SQLException {
		Long expected = request.getExpectedVersion();
		if (expected != null && expected == 0) {
			Savepoint savepoint = connection.setSavepoint();
			try { insertChain(connection, request); return 1; }
			catch (SQLException e) {
				if (!isConstraintViolation(e)) { throw e; }
				connection.rollback(savepoint);
				throw conflict("chain", request.getChainId(), expected,
						currentVersion(connection, dialect.chainTable(), "chain_id", request.getChainId()));
			}
		}

		int updated = updateChain(connection, request, expected);
		if (updated == 0) {
			long current = currentVersion(connection, dialect.chainTable(), "chain_id", request.getChainId());
			if (expected != null) { throw conflict("chain", request.getChainId(), expected, current); }
			Savepoint savepoint = connection.setSavepoint();
			try { insertChain(connection, request); return 1; }
			catch (SQLException e) {
				if (!isConstraintViolation(e)) { throw e; }
				connection.rollback(savepoint);
				if (updateChain(connection, request, null) != 1) { throw e; }
				return currentVersion(connection, dialect.chainTable(), "chain_id", request.getChainId());
			}
		}
		return expected == null
				? currentVersion(connection, dialect.chainTable(), "chain_id", request.getChainId()) : expected + 1;
	}

	private int updateChain(Connection connection, PublishChainRequest request, Long expected) throws SQLException {
		StringBuilder sql = new StringBuilder("UPDATE ").append(dialect.chainTable())
				.append(" SET el_data = ?, route_data = ?, namespace = ?, content_md5 = ?, ")
				.append("version = version + 1, enable = TRUE, gmt_modified = CURRENT_TIMESTAMP")
				.append(" WHERE application_name = ? AND chain_id = ?");
		if (expected != null) { sql.append(" AND version = ?"); }
		try (PreparedStatement statement = connection.prepareStatement(sql.toString())) {
			statement.setString(1, request.getEl());
			statement.setString(2, request.getRoute());
			statement.setString(3, request.getNamespace());
			statement.setString(4, SecureUtil.md5(request.getEl()));
			statement.setString(5, applicationName);
			statement.setString(6, request.getChainId());
			if (expected != null) { statement.setLong(7, expected); }
			return statement.executeUpdate();
		}
	}

	private void insertChain(Connection connection, PublishChainRequest request) throws SQLException {
		String sql = "INSERT INTO " + dialect.chainTable()
				+ " (application_name, chain_id, namespace, el_data, route_data, content_md5, version, enable)"
				+ " VALUES (?, ?, ?, ?, ?, ?, 1, TRUE)";
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
			Savepoint savepoint = connection.setSavepoint();
			try { insertScript(connection, request); return 1; }
			catch (SQLException e) {
				if (!isConstraintViolation(e)) { throw e; }
				connection.rollback(savepoint);
				throw conflict("script", request.getNodeId(), expected,
						currentVersion(connection, dialect.scriptTable(), "node_id", request.getNodeId()));
			}
		}

		int updated = updateScript(connection, request, expected);
		if (updated == 0) {
			long current = currentVersion(connection, dialect.scriptTable(), "node_id", request.getNodeId());
			if (expected != null) { throw conflict("script", request.getNodeId(), expected, current); }
			Savepoint savepoint = connection.setSavepoint();
			try { insertScript(connection, request); return 1; }
			catch (SQLException e) {
				if (!isConstraintViolation(e)) { throw e; }
				connection.rollback(savepoint);
				if (updateScript(connection, request, null) != 1) { throw e; }
				return currentVersion(connection, dialect.scriptTable(), "node_id", request.getNodeId());
			}
		}
		return expected == null
				? currentVersion(connection, dialect.scriptTable(), "node_id", request.getNodeId()) : expected + 1;
	}

	private int updateScript(Connection connection, PublishScriptRequest request, Long expected) throws SQLException {
		StringBuilder sql = new StringBuilder("UPDATE ").append(dialect.scriptTable())
				.append(" SET script_data = ?, script_name = ?, script_type = ?, script_language = ?, ")
				.append("content_md5 = ?, version = version + 1, enable = TRUE, gmt_modified = CURRENT_TIMESTAMP")
				.append(" WHERE application_name = ? AND node_id = ?");
		if (expected != null) { sql.append(" AND version = ?"); }
		try (PreparedStatement statement = connection.prepareStatement(sql.toString())) {
			statement.setString(1, request.getScript());
			statement.setString(2, request.getName());
			statement.setString(3, request.getType());
			statement.setString(4, request.getLanguage());
			statement.setString(5, SecureUtil.md5(request.getScript()));
			statement.setString(6, applicationName);
			statement.setString(7, request.getNodeId());
			if (expected != null) { statement.setLong(8, expected); }
			return statement.executeUpdate();
		}
	}

	private void insertScript(Connection connection, PublishScriptRequest request) throws SQLException {
		String sql = "INSERT INTO " + dialect.scriptTable()
				+ " (application_name, node_id, script_data, script_name, script_type, script_language, content_md5, version, enable)"
				+ " VALUES (?, ?, ?, ?, ?, ?, ?, 1, TRUE)";
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

	private PublishResult remove(RemoveRuleRequest request, ChangeRecord.TargetType type,
			String table, String idColumn) {
		return inTransaction("remove " + type.name().toLowerCase(), connection -> {
			long current = selectVersion(connection, table, idColumn, request.getTargetId(), true);
			Long expected = request.getExpectedVersion();
			if (expected != null && (expected == 0 || expected != current)) {
				throw conflict(type.name().toLowerCase(), request.getTargetId(), expected, current);
			}
			if (current > 0) {
				String sql = "DELETE FROM " + table + " WHERE application_name = ? AND " + idColumn + " = ? AND version = ?";
				try (PreparedStatement statement = connection.prepareStatement(sql)) {
					statement.setString(1, applicationName);
					statement.setString(2, request.getTargetId());
					statement.setLong(3, current);
					if (statement.executeUpdate() != 1) {
						throw conflict(type.name().toLowerCase(), request.getTargetId(),
								expected == null ? current : expected, current);
					}
				}
			}
			long sequence = insertChangeLog(connection, type, request.getTargetId(), ChangeRecord.Op.DELETE, current);
			return result(request.getTargetId(), type, ChangeRecord.Op.DELETE, current, sequence);
		});
	}

	private long currentVersion(Connection connection, String table, String idColumn, String id) throws SQLException {
		return selectVersion(connection, table, idColumn, id, false);
	}

	private long selectVersion(Connection connection, String table, String idColumn, String id,
			boolean forUpdate) throws SQLException {
		String sql = "SELECT version FROM " + table + " WHERE application_name = ? AND " + idColumn + " = ?"
				+ (forUpdate ? " FOR UPDATE" : "");
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setString(1, applicationName);
			statement.setString(2, id);
			try (ResultSet resultSet = statement.executeQuery()) {
				return resultSet.next() ? resultSet.getLong(1) : 0;
			}
		}
	}

	private long insertChangeLog(Connection connection, ChangeRecord.TargetType type, String id,
			ChangeRecord.Op operation, long version) throws SQLException {
		String sql = "INSERT INTO " + dialect.changeLogTable()
				+ " (application_name, target_type, target_id, op, version) VALUES (?, ?, ?, ?, ?) RETURNING seq";
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setString(1, applicationName);
			statement.setString(2, type.name());
			statement.setString(3, id);
			statement.setString(4, operation.name());
			statement.setLong(5, version);
			try (ResultSet resultSet = statement.executeQuery()) {
				if (resultSet.next()) { return resultSet.getLong(1); }
			}
		}
		throw new SQLException("change log insert did not return a sequence");
	}

	private PublishResult inTransaction(String operation, Work work) {
		try (Connection connection = connectionManager.getConnection()) {
			connection.setAutoCommit(false);
			try {
				PublishResult result = work.execute(connection);
				connection.commit();
				return result;
			}
			catch (RuntimeException | SQLException e) {
				try { connection.rollback(); } catch (SQLException ignored) { }
				if (e instanceof RuntimeException) { throw (RuntimeException) e; }
				throw new RuleStorageException("PostgreSQL " + operation + " failed: " + e.getMessage(), e);
			}
		}
		catch (SQLException e) {
			throw new RuleStorageException("PostgreSQL " + operation + " failed: " + e.getMessage(), e);
		}
	}

	private PublishResult result(String id, ChangeRecord.TargetType type, ChangeRecord.Op operation,
			long version, long sequence) {
		return PublishResult.builder().targetId(id).targetType(type).operation(operation)
				.version(version).sequence(sequence).build();
	}

	private VersionConflictException conflict(String type, String id, long expected, long current) {
		return new VersionConflictException(type + "[" + id + "] expected version[" + expected
				+ "] but current version is[" + current + "]");
	}

	private boolean isConstraintViolation(SQLException e) {
		return e.getSQLState() != null && e.getSQLState().startsWith("23");
	}

	@FunctionalInterface
	private interface Work { PublishResult execute(Connection connection) throws SQLException; }
}
