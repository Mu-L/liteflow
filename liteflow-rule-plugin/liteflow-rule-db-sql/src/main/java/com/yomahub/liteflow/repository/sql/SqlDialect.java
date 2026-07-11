package com.yomahub.liteflow.repository.sql;

import cn.hutool.core.io.resource.ResourceUtil;
import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbConfig;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 表名拼装（前缀可配，字段名固定）+ 建表 DDL。
 *
 * @author Bryan.Zhang
 * @since 2.16.1
 */
public class SqlDialect {

	public String prefix() {
		RuleDbConfig cfg = LiteflowConfigGetter.get().getRuleDb();
		String p = cfg == null ? null : cfg.getTablePrefix();
		return StrUtil.isBlank(p) ? "lf_" : p;
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

	/** 按连接的数据库产品选择 DDL 资源：MySQL 用 mysql 版（TEXT/KEY 语法），其余用 h2/通用版 */
	private String ddlResource(Connection conn) {
		try {
			String product = conn.getMetaData().getDatabaseProductName();
			if (product != null && product.toLowerCase().contains("mysql")) {
				return "sql/ddl-mysql.sql";
			}
		} catch (SQLException ignored) {
		}
		return "sql/ddl-h2.sql";
	}

	/** DDL 文本（缺表报错时给用户复制执行） */
	public String ddlText(Connection conn) {
		return applyPrefix(ResourceUtil.readUtf8Str(ddlResource(conn)));
	}

	private String applyPrefix(String tpl) {
		return tpl.replace("${prefix}", prefix());
	}

	/** auto-init-table=true 时建表 */
	public void createTablesIfAbsent(Connection conn) throws SQLException {
		String ddl = ddlText(conn);
		try (Statement st = conn.createStatement()) {
			for (String stmt : ddl.split(";")) {
				if (StrUtil.isNotBlank(stmt)) {
					st.execute(stmt);
				}
			}
		}
	}

	/** 未开 auto-init-table 时探测三张表；缺表抛 ConfigErrorException，附完整可复制执行的 DDL（spec §9） */
	public void checkTablesExist(Connection conn) {
		for (String table : new String[] {chainTable(), scriptTable(), changeLogTable()}) {
			try (Statement st = conn.createStatement()) {
				st.executeQuery("SELECT 1 FROM " + table + " WHERE 1=0");
			} catch (SQLException e) {
				throw new ConfigErrorException(StrUtil.format(
						"rule-db sql: table [{}] not found ({}). Create the tables with the DDL below, "
								+ "or set liteflow.rule-db.auto-init-table=true:\n{}",
						table, e.getMessage(), ddlText(conn)));
			}
		}
	}
}
