package com.yomahub.liteflow.repository.sql;

import cn.hutool.core.io.resource.ResourceUtil;
import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbConfig;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;

/** Table naming and DDL selection for one SQL backend instance. */
public class SqlDialect {

	private final String configuredPrefix;

	private final boolean dynamicExecutionPrefix;

	public SqlDialect() {
		this.configuredPrefix = null;
		this.dynamicExecutionPrefix = true;
	}

	SqlDialect(String prefix) {
		this.configuredPrefix = prefix;
		this.dynamicExecutionPrefix = false;
	}

	public String prefix() {
		String prefix = dynamicExecutionPrefix ? executionPrefix() : configuredPrefix;
		String resolved = StrUtil.isBlank(prefix) ? "lf_" : prefix;
		SqlStorageValidator.validateTablePrefix(resolved);
		return resolved;
	}

	public String chainTable() {
		return prefix() + "chain";
	}

	public String scriptTable() {
		return prefix() + "script";
	}

	public String changeLogTable() {
		return prefix() + "change_log";
	}

	public String changeLockTable() {
		return prefix() + "change_lock";
	}

	private String ddlResource(Connection conn) {
		try {
			String product = conn.getMetaData().getDatabaseProductName();
			String normalized = product == null ? "" : product.toLowerCase(Locale.ROOT);
			if (normalized.contains("mysql") || normalized.contains("mariadb")) {
				return "sql/ddl-mysql.sql";
			}
			if (normalized.contains("h2")) {
				return "sql/ddl-h2.sql";
			}
			throw new ConfigErrorException("rule-db sql database [" + product
					+ "] is unsupported; supported databases are MySQL and MariaDB (H2 is for tests)");
		}
		catch (SQLException e) {
			throw new ConfigErrorException("rule-db sql cannot detect the database dialect: " + e.getMessage());
		}
	}

	public String ddlText(Connection conn) {
		return ResourceUtil.readUtf8Str(ddlResource(conn)).replace("${prefix}", prefix());
	}

	public void createTablesIfAbsent(Connection conn) throws SQLException {
		String ddl = ddlText(conn);
		try (Statement statement = conn.createStatement()) {
			for (String item : ddl.split(";")) {
				if (StrUtil.isNotBlank(item)) {
					statement.execute(item);
				}
			}
		}
	}

	public void checkTablesExist(Connection conn) {
		// Fail closed even when users pre-create tables and disable automatic DDL.
		ddlResource(conn);
		checkTable(conn, chainTable(),
				"application_name, chain_id, namespace, el_data, route_data, version, "
						+ "content_md5, enable, gmt_create, gmt_modified",
				new int[][] { { 1, 64 }, { 2, 128 }, { 3, 64 }, { 7, 32 } });
		checkTable(conn, scriptTable(),
				"application_name, node_id, script_name, script_type, script_language, script_data, "
						+ "version, content_md5, enable, gmt_create, gmt_modified",
				new int[][] { { 1, 64 }, { 2, 128 }, { 3, 128 }, { 4, 32 }, { 5, 32 }, { 8, 32 } });
		checkTable(conn, changeLogTable(),
				"seq, application_name, target_type, target_id, op, version, gmt_create",
				new int[][] { { 2, 64 }, { 3, 16 }, { 4, 128 }, { 5, 16 } });
	}

	private void checkTable(Connection conn, String table, String columns, int[][] minimumPrecisions) {
		try (Statement statement = conn.createStatement();
				ResultSet resultSet = statement.executeQuery("SELECT " + columns + " FROM " + table + " WHERE 1=0")) {
			ResultSetMetaData metadata = resultSet.getMetaData();
			for (int[] requirement : minimumPrecisions) {
				int actual = metadata.getPrecision(requirement[0]);
				if (actual < requirement[1]) {
					throw new SQLException("column " + metadata.getColumnName(requirement[0])
							+ " capacity " + actual + " is below required " + requirement[1]);
				}
			}
		}
		catch (SQLException e) {
			throw new ConfigErrorException(StrUtil.format(
					"rule-db sql: table [{}] is missing or incompatible ({}). Create or migrate the tables "
							+ "with the DDL below, "
							+ "or set liteflow.rule-db.sql.auto-init-table=true:\n{}",
						table, e.getMessage(), ddlText(conn)));
		}
	}

	private static String executionPrefix() {
		RuleDbConfig config = LiteflowConfigGetter.get().getRuleDb();
		return config == null || config.getSql() == null ? null : config.getSql().getTablePrefix();
	}
}
