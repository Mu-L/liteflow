package com.yomahub.liteflow.repository.sql;

import com.yomahub.liteflow.exception.ConfigErrorException;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SqlDialectSupportTest {

	@Test
	void precreatedTablesDoNotBypassTheSupportedDatabaseCheck() throws Exception {
		Connection connection = mock(Connection.class);
		DatabaseMetaData metadata = mock(DatabaseMetaData.class);
		when(connection.getMetaData()).thenReturn(metadata);
		when(metadata.getDatabaseProductName()).thenReturn("PostgreSQL");

		ConfigErrorException error = assertThrows(ConfigErrorException.class,
				() -> new SqlDialect("lf_").checkTablesExist(connection));
		assertTrue(error.getMessage().contains("unsupported"));
		assertTrue(error.getMessage().contains("MySQL and MariaDB"));
	}
}
