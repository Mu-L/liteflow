package com.yomahub.liteflow.repository.postgresql;

import com.yomahub.liteflow.publisher.PublisherBackend;
import com.yomahub.liteflow.publisher.RulePublisherConfig;

import javax.sql.DataSource;

/** Type-safe connection configuration for an independent PostgreSQL publisher. */
public final class PostgresqlPublisherConfig implements RulePublisherConfig {

	private final String applicationName;
	private final String url;
	private final String username;
	private final String password;
	private final String driverClassName;
	private final String tablePrefix;
	private final DataSource dataSource;

	private PostgresqlPublisherConfig(Builder builder) {
		this.applicationName = builder.applicationName;
		this.url = builder.url;
		this.username = builder.username;
		this.password = builder.password;
		this.driverClassName = builder.driverClassName;
		this.tablePrefix = builder.tablePrefix;
		this.dataSource = builder.dataSource;
	}

	public static Builder builder() {
		return new Builder();
	}

	@Override
	public String applicationName() {
		return applicationName;
	}

	@Override
	public PublisherBackend backend() {
		return PublisherBackend.POSTGRESQL;
	}

	public String getUrl() {
		return url;
	}

	public String getUsername() {
		return username;
	}

	public String getPassword() {
		return password;
	}

	public String getDriverClassName() {
		return driverClassName;
	}

	public String getTablePrefix() {
		return tablePrefix;
	}

	public DataSource getDataSource() {
		return dataSource;
	}

	public static final class Builder {

		private String applicationName;
		private String url;
		private String username;
		private String password;
		private String driverClassName;
		private String tablePrefix = "lf_";
		private DataSource dataSource;

		private Builder() {
		}

		public Builder applicationName(String applicationName) {
			this.applicationName = applicationName;
			return this;
		}

		public Builder url(String url) {
			this.url = url;
			return this;
		}

		public Builder username(String username) {
			this.username = username;
			return this;
		}

		public Builder password(String password) {
			this.password = password;
			return this;
		}

		public Builder driverClassName(String driverClassName) {
			this.driverClassName = driverClassName;
			return this;
		}

		public Builder tablePrefix(String tablePrefix) {
			this.tablePrefix = tablePrefix;
			return this;
		}

		public Builder dataSource(DataSource dataSource) {
			this.dataSource = dataSource;
			return this;
		}

		public PostgresqlPublisherConfig build() {
			return new PostgresqlPublisherConfig(this);
		}
	}
}
