package com.yomahub.liteflow.repository.sql;

import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.exception.SeqGapException;
import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.repository.RuleRepository;
import com.yomahub.liteflow.repository.vo.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Rule-DB 的 SQL 权威源实现。
 *
 * @author Bryan.Zhang
 * @since 2.16.1
 */
public class SqlRuleRepository implements RuleRepository {

	private final SqlConnectionManager connectionManager;

	private final SqlDialect dialect;

	private final String applicationName;

	private final boolean autoInitTable;

	private final boolean dynamicExecutionConfig;

	private volatile boolean tableChecked = false;

	public SqlRuleRepository() {
		this.connectionManager = new SqlConnectionManager();
		this.dialect = new SqlDialect();
		this.applicationName = null;
		this.autoInitTable = false;
		this.dynamicExecutionConfig = true;
	}

	SqlRuleRepository(SqlConnectionManager connectionManager, SqlDialect dialect,
			String applicationName, boolean autoInitTable) {
		this.connectionManager = connectionManager;
		this.dialect = dialect;
		this.applicationName = StrUtil.isBlank(applicationName) ? "default" : applicationName;
		this.autoInitTable = autoInitTable;
		this.dynamicExecutionConfig = false;
	}

	private String app() {
		if (!dynamicExecutionConfig) {
			return applicationName;
		}
		RuleDbConfig config = LiteflowConfigGetter.get().getRuleDb();
		String name = config == null ? null : config.getApplicationName();
		return StrUtil.isBlank(name) ? "default" : name;
	}

	private Connection conn() throws SQLException {
		Connection c = connectionManager.getConnection();
		try {
			ensureTables(c);
		} catch (RuntimeException e) {
			// 建表失败时关闭已借出的连接，避免泄漏（调用方的 try-with-resources 尚未接管）
			try {
				c.close();
			} catch (Exception ignored) {
			}
			throw e;
		}
		return c;
	}

	private synchronized void ensureTables(Connection c) {
		if (tableChecked) {
			return;
		}
		if (autoInitTable()) {
			try {
				dialect.createTablesIfAbsent(c);
			} catch (SQLException e) {
				throw new RuntimeException("auto init rule-db tables failed: " + e.getMessage(), e);
			}
		} else {
			// 未开自动建表：显式探测三张表，缺表时报错并附完整 DDL，而非让后续查询抛裸 SQLException
			dialect.checkTablesExist(c);
		}
		tableChecked = true;
	}

	@Override
	public RuleManifest fetchManifest() {
		RuleManifest manifest = new RuleManifest();
		List<ChainMeta> chains = new ArrayList<>();
		List<ScriptMeta> scripts = new ArrayList<>();
		String chainSql = "SELECT chain_id, version, content_md5 FROM " + dialect.chainTable()
				+ " WHERE application_name = ? AND enable = 1";
		String scriptSql = "SELECT node_id, version, content_md5, script_type, script_language, script_name FROM "
				+ dialect.scriptTable() + " WHERE application_name = ? AND enable = 1";
		try (Connection c = conn()) {
			c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
			c.setAutoCommit(false);
			try (PreparedStatement ps = c.prepareStatement(chainSql)) {
				ps.setString(1, app());
				try (ResultSet rs = ps.executeQuery()) {
					while (rs.next()) {
						chains.add(new ChainMeta(rs.getString(1), rs.getLong(2), rs.getString(3)));
					}
				}
			}
			try (PreparedStatement ps = c.prepareStatement(scriptSql)) {
				ps.setString(1, app());
				try (ResultSet rs = ps.executeQuery()) {
					while (rs.next()) {
						scripts.add(new ScriptMeta(rs.getString(1), rs.getLong(2), rs.getString(3),
								rs.getString(4), rs.getString(5), rs.getString(6)));
					}
				}
			}
			manifest.setChains(chains);
			manifest.setScripts(scripts);
			// 内联 MAX(seq) 查询复用当前连接 c，不再调 fetchLatestSeq()（后者会再借一条连接，
			// 在 HikariCP maximumPoolSize=1 时与已持有的 c 自死锁）
			String seqSql = "SELECT MAX(seq) FROM " + dialect.changeLogTable() + " WHERE application_name = ?";
			try (PreparedStatement seqPs = c.prepareStatement(seqSql)) {
				seqPs.setString(1, app());
				try (ResultSet rs = seqPs.executeQuery()) {
					if (rs.next()) {
						long v = rs.getLong(1);
						manifest.setLatestSeq(rs.wasNull() ? 0 : v);
					} else {
						manifest.setLatestSeq(0);
					}
				}
			}
			c.commit();
		} catch (SQLException e) {
			throw wrap("fetchManifest", e);
		}
		return manifest;
	}

	@Override
	public ChainRecord fetchChain(String chainId) {
		String sql = "SELECT chain_id, el_data, route_data, namespace, version, content_md5, enable FROM "
				+ dialect.chainTable() + " WHERE application_name = ? AND chain_id = ?";
		try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql)) {
			ps.setString(1, app());
			ps.setString(2, chainId);
			try (ResultSet rs = ps.executeQuery()) {
				if (!rs.next()) {
					return null;
				}
				ChainRecord r = new ChainRecord();
				r.setChainId(rs.getString(1));
				r.setEl(rs.getString(2));
				r.setRoute(rs.getString(3));
				r.setNamespace(rs.getString(4));
				r.setVersion(rs.getLong(5));
				r.setMd5(rs.getString(6));
				r.setEnable(rs.getInt(7) == 1);
				return r;
			}
		} catch (SQLException e) {
			throw wrap("fetchChain", e);
		}
	}

	@Override
	public ChainMeta fetchChainMeta(String chainId) {
		String sql = "SELECT chain_id, version, content_md5 FROM " + dialect.chainTable()
				+ " WHERE application_name = ? AND chain_id = ? AND enable = 1";
		try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql)) {
			ps.setString(1, app());
			ps.setString(2, chainId);
			try (ResultSet rs = ps.executeQuery()) {
				if (!rs.next()) {
					return null;
				}
				return new ChainMeta(rs.getString(1), rs.getLong(2), rs.getString(3));
			}
		}
		catch (SQLException e) {
			throw wrap("fetchChainMeta", e);
		}
	}

	@Override
	public ScriptRecord fetchScript(String nodeId) {
		String sql = "SELECT node_id, script_data, script_name, script_type, script_language, version, content_md5, enable FROM "
				+ dialect.scriptTable() + " WHERE application_name = ? AND node_id = ?";
		try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql)) {
			ps.setString(1, app());
			ps.setString(2, nodeId);
			try (ResultSet rs = ps.executeQuery()) {
				if (!rs.next()) {
					return null;
				}
				ScriptRecord r = new ScriptRecord();
				r.setNodeId(rs.getString(1));
				r.setScript(rs.getString(2));
				r.setName(rs.getString(3));
				r.setType(rs.getString(4));
				r.setLanguage(rs.getString(5));
				r.setVersion(rs.getLong(6));
				r.setMd5(rs.getString(7));
				r.setEnable(rs.getInt(8) == 1);
				return r;
			}
		} catch (SQLException e) {
			throw wrap("fetchScript", e);
		}
	}

	@Override
	public ScriptMeta fetchScriptMeta(String nodeId) {
		String sql = "SELECT node_id, version, content_md5, script_type, script_language, script_name FROM "
				+ dialect.scriptTable() + " WHERE application_name = ? AND node_id = ? AND enable = 1";
		try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql)) {
			ps.setString(1, app());
			ps.setString(2, nodeId);
			try (ResultSet rs = ps.executeQuery()) {
				if (!rs.next()) {
					return null;
				}
				return new ScriptMeta(rs.getString(1), rs.getLong(2), rs.getString(3),
						rs.getString(4), rs.getString(5), rs.getString(6));
			}
		} catch (SQLException e) {
			throw wrap("fetchScriptMeta", e);
		}
	}

	public long fetchLatestSeq() {
		String sql = "SELECT MAX(seq) FROM " + dialect.changeLogTable() + " WHERE application_name = ?";
		try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql)) {
			ps.setString(1, app());
			try (ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					long v = rs.getLong(1);
					return rs.wasNull() ? 0 : v;
				}
				return 0;
			}
		} catch (SQLException e) {
			throw wrap("fetchLatestSeq", e);
		}
	}

	public List<ChangeRecord> fetchChangesSince(long seq) {
		// 断档检测：若存在记录但最小 seq 已大于 seq+1，说明中间被清理
		String minSql = "SELECT MIN(seq) FROM " + dialect.changeLogTable() + " WHERE application_name = ?";
		String listSql = "SELECT seq, target_type, target_id, op, version FROM " + dialect.changeLogTable()
				+ " WHERE application_name = ? AND seq > ? ORDER BY seq ASC";
		try (Connection c = conn()) {
			try (PreparedStatement ps = c.prepareStatement(minSql)) {
				ps.setString(1, app());
				try (ResultSet rs = ps.executeQuery()) {
					if (rs.next()) {
						long min = rs.getLong(1);
						if (!rs.wasNull() && seq > 0 && min > seq + 1) {
							throw new SeqGapException("change log gap: since=" + seq + " min=" + min);
						}
					}
				}
			}
			List<ChangeRecord> result = new ArrayList<>();
			try (PreparedStatement ps = c.prepareStatement(listSql)) {
				ps.setString(1, app());
				ps.setLong(2, seq);
				try (ResultSet rs = ps.executeQuery()) {
					while (rs.next()) {
						result.add(new ChangeRecord(rs.getLong(1),
								ChangeRecord.TargetType.valueOf(rs.getString(2)),
								rs.getString(3), ChangeRecord.Op.valueOf(rs.getString(4)), rs.getLong(5)));
					}
				}
			}
			validateSeqContinuity(result, seq);
			return result;
		} catch (SQLException e) {
			throw wrap("fetchChangesSince", e);
		}
	}

	private static void validateSeqContinuity(List<ChangeRecord> changes, long currentSeq) {
		long expected = currentSeq + 1;
		long previous = Long.MIN_VALUE;
		for (ChangeRecord change : changes) {
			long value = change.getSeq();
			if (value <= currentSeq || value == previous) {
				continue;
			}
			if (value != expected) {
				throw new SeqGapException("change log internal gap: since=" + currentSeq
						+ " expected=" + expected + " actual=" + value);
			}
			previous = value;
			expected = value + 1;
		}
	}

	private RuntimeException wrap(String op, SQLException e) {
		return new RuntimeException("rule-db sql " + op + " failed: " + e.getMessage(), e);
	}

	private boolean autoInitTable() {
		if (!dynamicExecutionConfig) {
			return autoInitTable;
		}
		RuleDbConfig config = LiteflowConfigGetter.get().getRuleDb();
		return config != null && config.getSql() != null
				&& Boolean.TRUE.equals(config.getSql().getAutoInitTable());
	}
}
