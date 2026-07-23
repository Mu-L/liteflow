package com.yomahub.liteflow.repository.postgresql;

import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.property.RuleDbPostgresqlConfig;
import com.yomahub.liteflow.spi.holder.ContextAwareHolder;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/** Resolves PostgreSQL connections for one provider or publisher. */
final class PostgresqlConnectionManager {

	private final RuleDbPostgresqlConfig executionConfig;
	private final PostgresqlPublisherConfig publisherConfig;
	private volatile DataSource dataSource;
	private volatile boolean resolved;
	private volatile boolean driverLoaded;

	PostgresqlConnectionManager(RuleDbPostgresqlConfig config) {
		this.executionConfig = config;
		this.publisherConfig = null;
	}

	PostgresqlConnectionManager(PostgresqlPublisherConfig config) {
		this.executionConfig = null;
		this.publisherConfig = config;
		this.dataSource = config.getDataSource();
		this.resolved = true;
	}

	Connection getConnection() throws SQLException {
		DataSource source = resolveDataSource();
		if (source != null) { return source.getConnection(); }
		if (StrUtil.isBlank(url())) {
			throw new ConfigErrorException("rule-db postgresql: neither a DataSource nor url is available");
		}
		loadDriver();
		return DriverManager.getConnection(url(), username(), password());
	}

	private DataSource resolveDataSource() {
		if (resolved) { return dataSource; }
		synchronized (this) {
			if (resolved) { return dataSource; }
			if (StrUtil.isBlank(url())) {
				DataSource candidate = lookupDataSourceBean(executionConfig.getDatasourceBeanName());
				// A failed lookup is not cached: the bean may appear later, so retry next time.
				if (candidate == null) { return null; }
				dataSource = candidate;
			}
			resolved = true;
			return dataSource;
		}
	}

	private DataSource lookupDataSourceBean(String beanName) {
		try {
			if (StrUtil.isNotBlank(beanName)) { return ContextAwareHolder.loadContextAware().getBean(beanName); }
			return ContextAwareHolder.loadContextAware().getBean(DataSource.class);
		}
		catch (Exception ignored) { return null; }
	}

	private void loadDriver() {
		if (driverLoaded) { return; }
		String driver = publisherConfig == null ? executionConfig.getDriverClassName() : publisherConfig.getDriverClassName();
		if (StrUtil.isBlank(driver)) { driver = "org.postgresql.Driver"; }
		try { Class.forName(driver); }
		catch (ClassNotFoundException e) {
			throw new ConfigErrorException("rule-db postgresql: driver class not found: " + driver);
		}
		driverLoaded = true;
	}

	private String url() { return publisherConfig == null ? executionConfig.getUrl() : publisherConfig.getUrl(); }
	private String username() { return publisherConfig == null ? executionConfig.getUsername() : publisherConfig.getUsername(); }
	private String password() { return publisherConfig == null ? executionConfig.getPassword() : publisherConfig.getPassword(); }
}
