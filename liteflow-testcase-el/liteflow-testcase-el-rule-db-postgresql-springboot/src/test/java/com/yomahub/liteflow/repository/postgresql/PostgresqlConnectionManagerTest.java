package com.yomahub.liteflow.repository.postgresql;

import com.yomahub.liteflow.core.proxy.DeclWarpBean;
import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.property.RuleDbPostgresqlConfig;
import com.yomahub.liteflow.spi.ContextAware;
import com.yomahub.liteflow.spi.holder.ContextAwareHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PostgresqlConnectionManagerTest {

	@AfterEach
	void resetContextAware() {
		ContextAwareHolder.clean();
	}

	@Test
	void failedDataSourceLookupIsRetriedInsteadOfCachedForever() throws Exception {
		StubContextAware contextAware = new StubContextAware();
		installContextAware(contextAware);
		PostgresqlConnectionManager manager = new PostgresqlConnectionManager(new RuleDbPostgresqlConfig());

		assertThrows(ConfigErrorException.class, manager::getConnection);

		DataSource dataSource = mock(DataSource.class);
		Connection connection = mock(Connection.class);
		when(dataSource.getConnection()).thenReturn(connection);
		contextAware.dataSource = dataSource;

		assertSame(connection, manager.getConnection());
		assertSame(connection, manager.getConnection());
		verify(dataSource, times(2)).getConnection();
	}

	@Test
	void namedDataSourceBeanIsResolvedWhenConfigured() throws Exception {
		StubContextAware contextAware = new StubContextAware();
		DataSource dataSource = mock(DataSource.class);
		Connection connection = mock(Connection.class);
		when(dataSource.getConnection()).thenReturn(connection);
		contextAware.dataSource = dataSource;
		installContextAware(contextAware);

		RuleDbPostgresqlConfig config = new RuleDbPostgresqlConfig();
		config.setDatasourceBeanName("primaryDs");
		PostgresqlConnectionManager manager = new PostgresqlConnectionManager(config);

		assertSame(connection, manager.getConnection());
		assertTrue(contextAware.requestedBeanNames.contains("primaryDs"));
	}

	@Test
	void blankUrlWithoutDataSourceRaisesConfigError() {
		PostgresqlConnectionManager manager = new PostgresqlConnectionManager(
				PostgresqlPublisherConfig.builder().applicationName("app").build());
		ConfigErrorException error = assertThrows(ConfigErrorException.class, manager::getConnection);
		assertTrue(error.getMessage().contains("neither a DataSource nor url"));
	}

	@Test
	void missingDriverClassRaisesConfigError() {
		RuleDbPostgresqlConfig config = new RuleDbPostgresqlConfig();
		config.setUrl("jdbc:postgresql://127.0.0.1:1/liteflow");
		config.setDriverClassName("com.example.DoesNotExistDriver");
		PostgresqlConnectionManager manager = new PostgresqlConnectionManager(config);
		ConfigErrorException error = assertThrows(ConfigErrorException.class, manager::getConnection);
		assertTrue(error.getMessage().contains("driver class not found"));
	}

	@Test
	void unreachableJdbcUrlSurfacesSqlExceptionOnEveryAttempt() {
		RuleDbPostgresqlConfig config = new RuleDbPostgresqlConfig();
		config.setUrl("jdbc:postgresql://127.0.0.1:1/liteflow");
		config.setUsername("u");
		config.setPassword("p");
		PostgresqlConnectionManager manager = new PostgresqlConnectionManager(config);
		assertThrows(SQLException.class, manager::getConnection);
		assertThrows(SQLException.class, manager::getConnection);
	}

	@Test
	void publisherConfigFallsBackToUrlWhenDataSourceIsAbsent() {
		PostgresqlPublisherConfig config = PostgresqlPublisherConfig.builder().applicationName("app")
				.url("jdbc:postgresql://127.0.0.1:1/liteflow").username("u").password("p").build();
		PostgresqlConnectionManager manager = new PostgresqlConnectionManager(config);
		assertThrows(SQLException.class, manager::getConnection);
	}

	private static void installContextAware(ContextAware contextAware) throws Exception {
		Field field = ContextAwareHolder.class.getDeclaredField("contextAware");
		field.setAccessible(true);
		field.set(null, contextAware);
	}

	private static final class StubContextAware implements ContextAware {

		private final java.util.List<String> requestedBeanNames = new java.util.ArrayList<>();
		private DataSource dataSource;

		@Override
		@SuppressWarnings("unchecked")
		public <T> T getBean(String name) {
			requestedBeanNames.add(name);
			if (dataSource != null) {
				return (T) dataSource;
			}
			throw new IllegalStateException("no bean named " + name);
		}

		@Override
		@SuppressWarnings("unchecked")
		public <T> T getBean(Class<T> clazz) {
			if (DataSource.class.isAssignableFrom(clazz) && dataSource != null) {
				return (T) dataSource;
			}
			throw new IllegalStateException("no bean of type " + clazz);
		}

		@Override
		public <T> T registerBean(String beanName, Class<T> clazz) {
			throw new UnsupportedOperationException();
		}

		@Override
		public <T> T registerBean(Class<T> clazz) {
			throw new UnsupportedOperationException();
		}

		@Override
		public <T> T registerBean(String beanName, Object bean) {
			throw new UnsupportedOperationException();
		}

		@Override
		public <T> T registerOrGet(String beanName, Class<T> clazz) {
			throw new UnsupportedOperationException();
		}

		@Override
		public <T> Map<String, T> getBeansOfType(Class<T> type) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean hasBean(String beanName) {
			return false;
		}

		@Override
		public boolean hasBean(Class<?> clazz) {
			return false;
		}

		@Override
		public Object registerDeclWrapBean(String beanName, DeclWarpBean declWarpBean) {
			throw new UnsupportedOperationException();
		}

		@Override
		public int priority() {
			return 0;
		}
	}
}
