package com.yomahub.liteflow.repository.sql;

import cn.hutool.core.io.resource.ResourceUtil;
import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbConfig;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

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
		return StrUtil.isBlank(prefix) ? "lf_" : prefix;
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

	private String ddlResource(Connection conn) {
		try {
			String product = conn.getMetaData().getDatabaseProductName();
			if (product != null && product.toLowerCase().contains("mysql")) {
				return "sql/ddl-mysql.sql";
			}
		}
		catch (SQLException ignored) {
		}
		return "sql/ddl-h2.sql";
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
		for (String table : new String[] { chainTable(), scriptTable(), changeLogTable() }) {
			try (Statement statement = conn.createStatement()) {
				statement.executeQuery("SELECT 1 FROM " + table + " WHERE 1=0");
			}
			catch (SQLException e) {
				throw new ConfigErrorException(StrUtil.format(
						"rule-db sql: table [{}] not found ({}). Create the tables with the DDL below, "
								+ "or set liteflow.rule-db.sql.auto-init-table=true:\n{}",
						table, e.getMessage(), ddlText(conn)));
			}
		}
	}

	private static String executionPrefix() {
		RuleDbConfig config = LiteflowConfigGetter.get().getRuleDb();
		return config == null || config.getSql() == null ? null : config.getSql().getTablePrefix();
	}
}
