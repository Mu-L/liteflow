package com.yomahub.liteflow.repository.sql;

import cn.hutool.core.io.resource.ResourceUtil;
import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbConfig;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 表名拼装（前缀可配，字段名固定）+ 建表 DDL。
 *
 * @author Bryan.Zhang
 * @since 2.16.2
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

	/** DDL 文本（缺表报错时给用户复制执行） */
	public String ddlText() {
		String tpl = ResourceUtil.readUtf8Str("sql/ddl-mysql.sql");
		return applyPrefix(tpl);
	}

	private String applyPrefix(String tpl) {
		return tpl.replace("${prefix}", prefix());
	}

	/** auto-init-table=true 时建表（用 h2/通用 DDL） */
	public void createTablesIfAbsent(Connection conn) throws SQLException {
		String ddl = applyPrefix(ResourceUtil.readUtf8Str("sql/ddl-h2.sql"));
		try (Statement st = conn.createStatement()) {
			for (String stmt : ddl.split(";")) {
				if (StrUtil.isNotBlank(stmt)) {
					st.execute(stmt);
				}
			}
		}
	}
}
