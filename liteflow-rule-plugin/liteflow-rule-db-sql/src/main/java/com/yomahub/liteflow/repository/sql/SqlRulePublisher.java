package com.yomahub.liteflow.repository.sql;

import cn.hutool.crypto.SecureUtil;
import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.repository.vo.ScriptRecord;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * SQL 发布 API：单事务内 UPSERT 内容行（version+1、重算 md5）+ INSERT change_log。
 * 可独立于 FlowExecutor 使用（管理后台只依赖本 jar）。
 *
 * @author Bryan.Zhang
 * @since 2.16.2
 */
public class SqlRulePublisher {

	private final SqlConnectionManager connectionManager = new SqlConnectionManager();

	private final SqlDialect dialect = new SqlDialect();

	private String app() {
		RuleDbConfig cfg = LiteflowConfigGetter.get().getRuleDb();
		return cfg == null || cfg.getApplicationName() == null ? "default" : cfg.getApplicationName();
	}

	public long publishChain(String chainId, String el) {
		String md5 = SecureUtil.md5(el);
		try (Connection c = connectionManager.getConnection()) {
			c.setAutoCommit(false);
			try {
				long version = upsertChain(c, chainId, el, md5);
				insertChangeLog(c, "CHAIN", chainId, "UPSERT", version);
				c.commit();
				return version;
			} catch (SQLException e) {
				c.rollback();
				throw e;
			}
		} catch (SQLException e) {
			throw new RuntimeException("publishChain failed: " + e.getMessage(), e);
		}
	}

	public long publishScript(ScriptRecord s) {
		String md5 = SecureUtil.md5(s.getScript());
		try (Connection c = connectionManager.getConnection()) {
			c.setAutoCommit(false);
			try {
				long version = upsertScript(c, s, md5);
				insertChangeLog(c, "SCRIPT", s.getNodeId(), "UPSERT", version);
				c.commit();
				return version;
			} catch (SQLException e) {
				c.rollback();
				throw e;
			}
		} catch (SQLException e) {
			throw new RuntimeException("publishScript failed: " + e.getMessage(), e);
		}
	}

	public void removeChain(String chainId) {
		try (Connection c = connectionManager.getConnection()) {
			c.setAutoCommit(false);
			try {
				long version = currentChainVersion(c, chainId);
				try (PreparedStatement ps = c.prepareStatement(
						"DELETE FROM " + dialect.chainTable() + " WHERE application_name = ? AND chain_id = ?")) {
					ps.setString(1, app());
					ps.setString(2, chainId);
					ps.executeUpdate();
				}
				insertChangeLog(c, "CHAIN", chainId, "DELETE", version);
				c.commit();
			} catch (SQLException e) {
				c.rollback();
				throw e;
			}
		} catch (SQLException e) {
			throw new RuntimeException("removeChain failed: " + e.getMessage(), e);
		}
	}

	public void removeScript(String nodeId) {
		try (Connection c = connectionManager.getConnection()) {
			c.setAutoCommit(false);
			try {
				long version = currentScriptVersion(c, nodeId);
				try (PreparedStatement ps = c.prepareStatement(
						"DELETE FROM " + dialect.scriptTable() + " WHERE application_name = ? AND node_id = ?")) {
					ps.setString(1, app());
					ps.setString(2, nodeId);
					ps.executeUpdate();
				}
				insertChangeLog(c, "SCRIPT", nodeId, "DELETE", version);
				c.commit();
			} catch (SQLException e) {
				c.rollback();
				throw e;
			}
		} catch (SQLException e) {
			throw new RuntimeException("removeScript failed: " + e.getMessage(), e);
		}
	}

	private long currentChainVersion(Connection c, String chainId) throws SQLException {
		try (PreparedStatement ps = c.prepareStatement(
				"SELECT version FROM " + dialect.chainTable() + " WHERE application_name = ? AND chain_id = ?")) {
			ps.setString(1, app());
			ps.setString(2, chainId);
			try (ResultSet rs = ps.executeQuery()) {
				return rs.next() ? rs.getLong(1) : 0;
			}
		}
	}

	private long currentScriptVersion(Connection c, String nodeId) throws SQLException {
		try (PreparedStatement ps = c.prepareStatement(
				"SELECT version FROM " + dialect.scriptTable() + " WHERE application_name = ? AND node_id = ?")) {
			ps.setString(1, app());
			ps.setString(2, nodeId);
			try (ResultSet rs = ps.executeQuery()) {
				return rs.next() ? rs.getLong(1) : 0;
			}
		}
	}

	/**
	 * UPSERT chain 行并返回新版本号。
	 * 已有行：{@code version = version + 1} 在行锁下原子自增（spec §7），同事务内 SELECT 取回新版本；
	 * 新行：插入 version=1。不再先 SELECT 再 Java+1（原 read-modify-write 在并发发布时丢更新）。
	 */
	private long upsertChain(Connection c, String chainId, String el, String md5) throws SQLException {
		int updated;
		try (PreparedStatement ps = c.prepareStatement("UPDATE " + dialect.chainTable()
				+ " SET el_data = ?, content_md5 = ?, version = version + 1, enable = 1, gmt_modified = CURRENT_TIMESTAMP"
				+ " WHERE application_name = ? AND chain_id = ?")) {
			ps.setString(1, el);
			ps.setString(2, md5);
			ps.setString(3, app());
			ps.setString(4, chainId);
			updated = ps.executeUpdate();
		}
		if (updated == 0) {
			try (PreparedStatement ps = c.prepareStatement("INSERT INTO " + dialect.chainTable()
					+ " (application_name, chain_id, el_data, content_md5, version, enable) VALUES (?, ?, ?, ?, 1, 1)")) {
				ps.setString(1, app());
				ps.setString(2, chainId);
				ps.setString(3, el);
				ps.setString(4, md5);
				ps.executeUpdate();
			}
			return 1;
		}
		// UPDATE 已原子自增，同事务内 SELECT 取回新版本供 change_log 使用
		return currentChainVersion(c, chainId);
	}

	/**
	 * UPSERT script 行并返回新版本号（同 {@link #upsertChain} 的原子自增策略，spec §7）。
	 */
	private long upsertScript(Connection c, ScriptRecord s, String md5) throws SQLException {
		int updated;
		try (PreparedStatement ps = c.prepareStatement("UPDATE " + dialect.scriptTable()
				+ " SET script_data = ?, script_name = ?, script_type = ?, script_language = ?, content_md5 = ?, version = version + 1, enable = 1, gmt_modified = CURRENT_TIMESTAMP"
				+ " WHERE application_name = ? AND node_id = ?")) {
			ps.setString(1, s.getScript());
			ps.setString(2, s.getName());
			ps.setString(3, s.getType());
			ps.setString(4, s.getLanguage());
			ps.setString(5, md5);
			ps.setString(6, app());
			ps.setString(7, s.getNodeId());
			updated = ps.executeUpdate();
		}
		if (updated == 0) {
			try (PreparedStatement ps = c.prepareStatement("INSERT INTO " + dialect.scriptTable()
					+ " (application_name, node_id, script_data, script_name, script_type, script_language, content_md5, version, enable) VALUES (?, ?, ?, ?, ?, ?, ?, 1, 1)")) {
				ps.setString(1, app());
				ps.setString(2, s.getNodeId());
				ps.setString(3, s.getScript());
				ps.setString(4, s.getName());
				ps.setString(5, s.getType());
				ps.setString(6, s.getLanguage());
				ps.setString(7, md5);
				ps.executeUpdate();
			}
			return 1;
		}
		return currentScriptVersion(c, s.getNodeId());
	}

	private void insertChangeLog(Connection c, String targetType, String targetId, String op, long version)
			throws SQLException {
		try (PreparedStatement ps = c.prepareStatement("INSERT INTO " + dialect.changeLogTable()
				+ " (application_name, target_type, target_id, op, version) VALUES (?, ?, ?, ?, ?)")) {
			ps.setString(1, app());
			ps.setString(2, targetType);
			ps.setString(3, targetId);
			ps.setString(4, op);
			ps.setLong(5, version);
			ps.executeUpdate();
		}
	}
}
