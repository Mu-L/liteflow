package com.yomahub.liteflow.repository.postgresql;

import cn.hutool.core.io.resource.ResourceUtil;
import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.exception.ConfigErrorException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;

/** PostgreSQL table naming, DDL, and schema validation. */
final class PostgresqlDialect {

	private final String prefix;

	PostgresqlDialect(String prefix) {
		this.prefix = StrUtil.isBlank(prefix) ? "lf_" : prefix;
		PostgresqlStorageValidator.validateTablePrefix(this.prefix);
	}

	String chainTable() { return prefix + "chain"; }
	String scriptTable() { return prefix + "script"; }
	String changeLogTable() { return prefix + "change_log"; }

	String ddlText(Connection connection) {
		assertPostgresql(connection);
		return ResourceUtil.readUtf8Str("postgresql/ddl.sql").replace("${prefix}", prefix);
	}

	void createTablesIfAbsent(Connection connection) throws SQLException {
		try (Statement statement = connection.createStatement()) {
			for (String item : ddlText(connection).split(";")) {
				if (StrUtil.isNotBlank(item)) { statement.execute(item); }
			}
		}
	}

	void checkTablesExist(Connection connection) {
		assertPostgresql(connection);
		checkTable(connection, chainTable(),
				"application_name, chain_id, namespace, el_data, route_data, version, content_md5, enable, gmt_create, gmt_modified",
				new int[][] {{1, 64}, {2, 128}, {3, 64}, {7, 32}});
		checkTable(connection, scriptTable(),
				"application_name, node_id, script_name, script_type, script_language, script_data, version, content_md5, enable, gmt_create, gmt_modified",
				new int[][] {{1, 64}, {2, 128}, {3, 128}, {4, 32}, {5, 32}, {8, 32}});
		checkTable(connection, changeLogTable(),
				"seq, application_name, target_type, target_id, op, version, gmt_create",
				new int[][] {{2, 64}, {3, 16}, {4, 128}, {5, 16}});
	}

	private void assertPostgresql(Connection connection) {
		try {
			String product = connection.getMetaData().getDatabaseProductName();
			if (product == null || !product.toLowerCase(Locale.ROOT).contains("postgresql")) {
				throw new ConfigErrorException("rule-db postgresql database [" + product + "] is unsupported");
			}
		}
		catch (SQLException e) {
			throw new ConfigErrorException("rule-db postgresql cannot detect database dialect: " + e.getMessage());
		}
	}

	private void checkTable(Connection connection, String table, String columns, int[][] required) {
		try (Statement statement = connection.createStatement();
				ResultSet resultSet = statement.executeQuery("SELECT " + columns + " FROM " + table + " WHERE 1=0")) {
			ResultSetMetaData metadata = resultSet.getMetaData();
			for (int[] item : required) {
				int actual = metadata.getPrecision(item[0]);
				if (actual < item[1]) {
					throw new SQLException("column " + metadata.getColumnName(item[0]) + " capacity " + actual
							+ " is below required " + item[1]);
				}
			}
		}
		catch (SQLException e) {
			throw new ConfigErrorException("rule-db postgresql: table [" + table + "] is missing or incompatible ("
					+ e.getMessage() + "). Create or migrate it with the module DDL");
		}
	}
}
