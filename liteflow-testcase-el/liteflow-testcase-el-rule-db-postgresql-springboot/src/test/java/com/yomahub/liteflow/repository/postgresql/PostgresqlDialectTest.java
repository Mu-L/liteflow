package com.yomahub.liteflow.repository.postgresql;

import com.yomahub.liteflow.exception.ConfigErrorException;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PostgresqlDialectTest {

	@Test
	void tableNamesUseThePrefixWithDefaultFallback() {
		PostgresqlDialect standard = new PostgresqlDialect(null);
		assertEquals("lf_chain", standard.chainTable());
		assertEquals("lf_script", standard.scriptTable());
		assertEquals("lf_change_log", standard.changeLogTable());

		PostgresqlDialect custom = new PostgresqlDialect("x_");
		assertEquals("x_chain", custom.chainTable());
	}

	@Test
	void invalidTablePrefixIsRejected() {
		assertThrows(ConfigErrorException.class, () -> new PostgresqlDialect("lf_;drop"));
	}

	@Test
	void nonPostgresqlConnectionsAreRejected() throws Exception {
		Connection connection = connectionNamed("MySQL");
		ConfigErrorException error = assertThrows(ConfigErrorException.class,
				() -> new PostgresqlDialect("lf_").checkTablesExist(connection));
		assertTrue(error.getMessage().contains("unsupported"));

		assertThrows(ConfigErrorException.class,
				() -> new PostgresqlDialect("lf_").ddlText(connection));
	}

	@Test
	void undetectableProductNameRaisesConfigError() throws Exception {
		Connection connection = mock(Connection.class);
		when(connection.getMetaData()).thenThrow(new SQLException("no metadata", "08003"));
		assertThrows(ConfigErrorException.class,
				() -> new PostgresqlDialect("lf_").checkTablesExist(connection));
	}

	@Test
	void missingTablesAreReportedWithMigrationHint() throws Exception {
		Connection connection = connectionNamed("PostgreSQL");
		Statement statement = mock(Statement.class);
		when(statement.executeQuery(anyString())).thenThrow(new SQLException("relation does not exist", "42P01"));
		when(connection.createStatement()).thenReturn(statement);

		ConfigErrorException error = assertThrows(ConfigErrorException.class,
				() -> new PostgresqlDialect("lf_").checkTablesExist(connection));
		assertTrue(error.getMessage().contains("missing or incompatible"));
	}

	@Test
	void undersizedColumnsAreReportedAsIncompatible() throws Exception {
		Connection connection = connectionNamed("PostgreSQL");
		Statement statement = mock(Statement.class);
		ResultSet empty = mock(ResultSet.class);
		ResultSetMetaData columns = mock(ResultSetMetaData.class);
		when(columns.getPrecision(anyInt())).thenReturn(8);
		when(columns.getColumnName(anyInt())).thenReturn("application_name");
		when(empty.getMetaData()).thenReturn(columns);
		when(statement.executeQuery(anyString())).thenReturn(empty);
		when(connection.createStatement()).thenReturn(statement);

		ConfigErrorException error = assertThrows(ConfigErrorException.class,
				() -> new PostgresqlDialect("lf_").checkTablesExist(connection));
		assertTrue(error.getMessage().contains("capacity"));
	}

	private Connection connectionNamed(String product) throws SQLException {
		Connection connection = mock(Connection.class);
		DatabaseMetaData databaseMetaData = mock(DatabaseMetaData.class);
		when(databaseMetaData.getDatabaseProductName()).thenReturn(product);
		when(connection.getMetaData()).thenReturn(databaseMetaData);
		return connection;
	}
}
