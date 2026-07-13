package com.yomahub.liteflow.repository.sql;

import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.property.RuleDbSqlConfig;
import com.yomahub.liteflow.spi.holder.ContextAwareHolder;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/** Resolves SQL connections for one execution provider or publisher instance. */
public class SqlConnectionManager {

	private final RuleDbSqlConfig executionConfig;
	private final SqlPublisherConfig publisherConfig;
	private final boolean dynamicExecutionConfig;

	private volatile DataSource dataSource;
	private volatile boolean resolved;

	public SqlConnectionManager() {
		this.executionConfig = null;
		this.publisherConfig = null;
		this.dynamicExecutionConfig = true;
	}

	SqlConnectionManager(RuleDbSqlConfig config) {
		this.executionConfig = config;
		this.publisherConfig = null;
		this.dynamicExecutionConfig = false;
	}

	SqlConnectionManager(SqlPublisherConfig config) {
		this.executionConfig = null;
		this.publisherConfig = config;
		this.dynamicExecutionConfig = false;
		this.dataSource = config.getDataSource();
		this.resolved = true;
	}

	public Connection getConnection() throws SQLException {
		DataSource resolvedDataSource = resolveDataSource();
		if (resolvedDataSource != null) {
			return resolvedDataSource.getConnection();
		}
		String url = url();
		if (StrUtil.isBlank(url)) {
			throw new ConfigErrorException(
					"rule-db sql: neither a DataSource nor liteflow.rule-db.sql.url is available");
		}
		loadDriverIfNeeded(url);
		return DriverManager.getConnection(url, username(), password());
	}

	private DataSource resolveDataSource() {
		if (resolved) {
			return dataSource;
		}
		synchronized (this) {
			if (resolved) {
				return dataSource;
			}
			if (publisherConfig == null && StrUtil.isBlank(url())) {
				dataSource = lookupDataSourceBean(execution().getDatasourceBeanName());
			}
			resolved = true;
			return dataSource;
		}
	}

	private DataSource lookupDataSourceBean(String beanName) {
		try {
			if (StrUtil.isNotBlank(beanName)) {
				return ContextAwareHolder.loadContextAware().getBean(beanName);
			}
			return ContextAwareHolder.loadContextAware().getBean(DataSource.class);
		}
		catch (Exception e) {
			return null;
		}
	}

	private void loadDriverIfNeeded(String url) {
		String driver = driverClassName();
		if (StrUtil.isBlank(driver)) {
			driver = guessDriver(url);
		}
		if (StrUtil.isNotBlank(driver)) {
			try {
				Class.forName(driver);
			}
			catch (ClassNotFoundException e) {
				throw new ConfigErrorException("rule-db sql: driver class not found: " + driver);
			}
		}
	}

	private String url() {
		return publisherConfig == null ? execution().getUrl() : publisherConfig.getUrl();
	}

	private String username() {
		return publisherConfig == null ? execution().getUsername() : publisherConfig.getUsername();
	}

	private String password() {
		return publisherConfig == null ? execution().getPassword() : publisherConfig.getPassword();
	}

	private String driverClassName() {
		return publisherConfig == null
				? execution().getDriverClassName() : publisherConfig.getDriverClassName();
	}

	private RuleDbSqlConfig execution() {
		if (!dynamicExecutionConfig) {
			return executionConfig;
		}
		RuleDbConfig config = LiteflowConfigGetter.get().getRuleDb();
		return config == null || config.getSql() == null ? new RuleDbSqlConfig() : config.getSql();
	}

	private String guessDriver(String jdbcUrl) {
		if (jdbcUrl == null) {
			return null;
		}
		if (jdbcUrl.startsWith("jdbc:mysql")) {
			return "com.mysql.cj.jdbc.Driver";
		}
		if (jdbcUrl.startsWith("jdbc:h2")) {
			return "org.h2.Driver";
		}
		if (jdbcUrl.startsWith("jdbc:postgresql")) {
			return "org.postgresql.Driver";
		}
		return null;
	}
}
