package com.yomahub.liteflow.repository.postgresql;

import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.repository.RuleRepository;
import com.yomahub.liteflow.repository.vo.ChainMeta;
import com.yomahub.liteflow.repository.vo.ChainRecord;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import com.yomahub.liteflow.repository.vo.RuleManifest;
import com.yomahub.liteflow.repository.vo.ScriptMeta;
import com.yomahub.liteflow.repository.vo.ScriptRecord;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/** PostgreSQL authoritative repository for Rule-DB. */
public class PostgresqlRuleRepository implements RuleRepository {

	private final PostgresqlConnectionManager connectionManager;
	private final PostgresqlDialect dialect;
	private final String applicationName;
	private final boolean autoInitTable;
	private volatile boolean tableChecked;

	PostgresqlRuleRepository(PostgresqlConnectionManager connectionManager, PostgresqlDialect dialect,
			String applicationName, boolean autoInitTable) {
		this.connectionManager = connectionManager;
		this.dialect = dialect;
		this.applicationName = PostgresqlStorageValidator.applicationNameOrDefault(applicationName);
		this.autoInitTable = autoInitTable;
	}

	@Override
	public RuleManifest fetchManifest() {
		RuleManifest manifest = new RuleManifest();
		List<ChainMeta> chains = new ArrayList<>();
		List<ScriptMeta> scripts = new ArrayList<>();
		try (Connection connection = connection()) {
			int previousIsolation = connection.getTransactionIsolation();
			boolean previousAutoCommit = connection.getAutoCommit();
			try {
				connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
				connection.setAutoCommit(false);
				try (PreparedStatement statement = connection.prepareStatement("SELECT chain_id, version, content_md5 FROM "
						+ dialect.chainTable() + " WHERE application_name = ? AND enable = TRUE")) {
					statement.setString(1, applicationName);
					try (ResultSet resultSet = statement.executeQuery()) {
						while (resultSet.next()) {
							chains.add(new ChainMeta(resultSet.getString(1), resultSet.getLong(2), resultSet.getString(3)));
						}
					}
				}
				try (PreparedStatement statement = connection.prepareStatement(
						"SELECT node_id, version, content_md5, script_type, script_language, script_name FROM "
								+ dialect.scriptTable() + " WHERE application_name = ? AND enable = TRUE")) {
					statement.setString(1, applicationName);
					try (ResultSet resultSet = statement.executeQuery()) {
						while (resultSet.next()) {
							scripts.add(new ScriptMeta(resultSet.getString(1), resultSet.getLong(2), resultSet.getString(3),
									resultSet.getString(4), resultSet.getString(5), resultSet.getString(6)));
						}
					}
				}
				manifest.setChains(chains);
				manifest.setScripts(scripts);
				manifest.setLatestSeq(latestSeq(connection));
				connection.commit();
			}
			finally { restoreConnectionState(connection, previousIsolation, previousAutoCommit); }
		}
		catch (SQLException e) { throw wrap("fetchManifest", e); }
		return manifest;
	}

	@Override
	public ChainRecord fetchChain(String chainId) {
		PostgresqlStorageValidator.validateTargetId("chainId", chainId);
		String sql = "SELECT chain_id, el_data, route_data, namespace, version, content_md5, enable FROM "
				+ dialect.chainTable() + " WHERE application_name = ? AND chain_id = ?";
		try (Connection connection = connection(); PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setString(1, applicationName);
			statement.setString(2, chainId);
			try (ResultSet resultSet = statement.executeQuery()) {
				if (!resultSet.next()) { return null; }
				ChainRecord record = new ChainRecord();
				record.setChainId(resultSet.getString(1));
				record.setEl(resultSet.getString(2));
				record.setRoute(resultSet.getString(3));
				record.setNamespace(resultSet.getString(4));
				record.setVersion(resultSet.getLong(5));
				record.setMd5(resultSet.getString(6));
				record.setEnable(resultSet.getBoolean(7));
				return record;
			}
		}
		catch (SQLException e) { throw wrap("fetchChain", e); }
	}

	@Override
	public ChainMeta fetchChainMeta(String chainId) {
		PostgresqlStorageValidator.validateTargetId("chainId", chainId);
		String sql = "SELECT chain_id, version, content_md5 FROM " + dialect.chainTable()
				+ " WHERE application_name = ? AND chain_id = ? AND enable = TRUE";
		try (Connection connection = connection(); PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setString(1, applicationName);
			statement.setString(2, chainId);
			try (ResultSet resultSet = statement.executeQuery()) {
				return resultSet.next()
						? new ChainMeta(resultSet.getString(1), resultSet.getLong(2), resultSet.getString(3)) : null;
			}
		}
		catch (SQLException e) { throw wrap("fetchChainMeta", e); }
	}

	@Override
	public ScriptRecord fetchScript(String nodeId) {
		PostgresqlStorageValidator.validateTargetId("nodeId", nodeId);
		String sql = "SELECT node_id, script_data, script_name, script_type, script_language, version, content_md5, enable FROM "
				+ dialect.scriptTable() + " WHERE application_name = ? AND node_id = ?";
		try (Connection connection = connection(); PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setString(1, applicationName);
			statement.setString(2, nodeId);
			try (ResultSet resultSet = statement.executeQuery()) {
				if (!resultSet.next()) { return null; }
				ScriptRecord record = new ScriptRecord();
				record.setNodeId(resultSet.getString(1));
				record.setScript(resultSet.getString(2));
				record.setName(resultSet.getString(3));
				record.setType(resultSet.getString(4));
				record.setLanguage(resultSet.getString(5));
				record.setVersion(resultSet.getLong(6));
				record.setMd5(resultSet.getString(7));
				record.setEnable(resultSet.getBoolean(8));
				return record;
			}
		}
		catch (SQLException e) { throw wrap("fetchScript", e); }
	}

	@Override
	public ScriptMeta fetchScriptMeta(String nodeId) {
		PostgresqlStorageValidator.validateTargetId("nodeId", nodeId);
		String sql = "SELECT node_id, version, content_md5, script_type, script_language, script_name FROM "
				+ dialect.scriptTable() + " WHERE application_name = ? AND node_id = ? AND enable = TRUE";
		try (Connection connection = connection(); PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setString(1, applicationName);
			statement.setString(2, nodeId);
			try (ResultSet resultSet = statement.executeQuery()) {
				return resultSet.next() ? new ScriptMeta(resultSet.getString(1), resultSet.getLong(2),
						resultSet.getString(3), resultSet.getString(4), resultSet.getString(5), resultSet.getString(6)) : null;
			}
		}
		catch (SQLException e) { throw wrap("fetchScriptMeta", e); }
	}

	public long fetchLatestSeq() {
		try (Connection connection = connection()) { return latestSeq(connection); }
		catch (SQLException e) { throw wrap("fetchLatestSeq", e); }
	}

	public List<ChangeRecord> fetchChangesSince(long sequence, int limit) {
		if (limit <= 0) { throw new ConfigErrorException("rule-db postgresql change-log batch size must be positive"); }
		String sql = "SELECT seq, target_type, target_id, op, version FROM " + dialect.changeLogTable()
				+ " WHERE application_name = ? AND seq > ? ORDER BY seq ASC LIMIT ?";
		try (Connection connection = connection(); PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setString(1, applicationName);
			statement.setLong(2, sequence);
			statement.setInt(3, limit);
			List<ChangeRecord> changes = new ArrayList<>();
			try (ResultSet resultSet = statement.executeQuery()) {
				while (resultSet.next()) {
					long seq = resultSet.getLong(1);
					try {
						changes.add(new ChangeRecord(seq, ChangeRecord.TargetType.valueOf(resultSet.getString(2)),
								resultSet.getString(3), ChangeRecord.Op.valueOf(resultSet.getString(4)), resultSet.getLong(5)));
					}
					catch (RuntimeException e) {
						throw new PostgresqlChangeLogCorruptionException(
								"rule-db postgresql change-log row[" + seq + "] is invalid", e);
					}
				}
			}
			return changes;
		}
		catch (SQLException e) { throw wrap("fetchChangesSince", e); }
	}

	private long latestSeq(Connection connection) throws SQLException {
		String sql = "SELECT MAX(seq) FROM " + dialect.changeLogTable() + " WHERE application_name = ?";
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setString(1, applicationName);
			try (ResultSet resultSet = statement.executeQuery()) {
				if (!resultSet.next()) { return 0; }
				long result = resultSet.getLong(1);
				return resultSet.wasNull() ? 0 : result;
			}
		}
	}

	private Connection connection() throws SQLException {
		Connection connection = connectionManager.getConnection();
		try { ensureTables(connection); return connection; }
		catch (RuntimeException e) {
			try { connection.close(); } catch (Exception ignored) { }
			throw e;
		}
	}

	private synchronized void ensureTables(Connection connection) {
		if (tableChecked) { return; }
		try {
			if (autoInitTable) { dialect.createTablesIfAbsent(connection); }
			dialect.checkTablesExist(connection);
			tableChecked = true;
		}
		catch (SQLException e) { throw new ConfigErrorException("auto init rule-db postgresql tables failed: " + e.getMessage()); }
	}

	private RuntimeException wrap(String operation, SQLException e) {
		return new RuntimeException("rule-db postgresql " + operation + " failed: " + e.getMessage(), e);
	}

	/** Returns a borrowed pooled connection with the state it had before this borrow. */
	private void restoreConnectionState(Connection connection, int isolation, boolean autoCommit) {
		// End the transaction first: PostgreSQL rejects isolation changes mid-transaction.
		try { connection.setAutoCommit(autoCommit); }
		catch (SQLException ignored) { }
		try { connection.setTransactionIsolation(isolation); }
		catch (SQLException ignored) { }
	}
}
