package com.yomahub.liteflow.repository.nacos;

import com.alibaba.nacos.api.config.ConfigService;
import com.yomahub.liteflow.publisher.PublisherBackend;
import com.yomahub.liteflow.publisher.RulePublisherConfig;

/** Type-safe configuration for an independent Nacos rule publisher. */
public final class NacosPublisherConfig implements RulePublisherConfig {

	private final String applicationName;
	private final String serverAddr;
	private final String namespace;
	private final String group;
	private final String dataIdPrefix;
	private final String username;
	private final String password;
	private final String accessKey;
	private final String secretKey;
	private final Long timeoutMillis;
	private final ConfigService configService;

	private NacosPublisherConfig(Builder builder) {
		this.applicationName = builder.applicationName;
		this.serverAddr = builder.serverAddr;
		this.namespace = builder.namespace;
		this.group = builder.group;
		this.dataIdPrefix = builder.dataIdPrefix;
		this.username = builder.username;
		this.password = builder.password;
		this.accessKey = builder.accessKey;
		this.secretKey = builder.secretKey;
		this.timeoutMillis = builder.timeoutMillis;
		this.configService = builder.configService;
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
		return PublisherBackend.NACOS;
	}

	public String getServerAddr() { return serverAddr; }
	public String getNamespace() { return namespace; }
	public String getGroup() { return group; }
	public String getDataIdPrefix() { return dataIdPrefix; }
	public String getUsername() { return username; }
	public String getPassword() { return password; }
	public String getAccessKey() { return accessKey; }
	public String getSecretKey() { return secretKey; }
	public Long getTimeoutMillis() { return timeoutMillis; }
	public ConfigService getConfigService() { return configService; }

	public static final class Builder {
		private String applicationName;
		private String serverAddr;
		private String namespace;
		private String group = "LITEFLOW_RULE_DB";
		private String dataIdPrefix = "liteflow-rule-db";
		private String username;
		private String password;
		private String accessKey;
		private String secretKey;
		private Long timeoutMillis = 3000L;
		private ConfigService configService;

		private Builder() { }

		public Builder applicationName(String value) { this.applicationName = value; return this; }
		public Builder serverAddr(String value) { this.serverAddr = value; return this; }
		public Builder namespace(String value) { this.namespace = value; return this; }
		public Builder group(String value) { this.group = value; return this; }
		public Builder dataIdPrefix(String value) { this.dataIdPrefix = value; return this; }
		public Builder username(String value) { this.username = value; return this; }
		public Builder password(String value) { this.password = value; return this; }
		public Builder accessKey(String value) { this.accessKey = value; return this; }
		public Builder secretKey(String value) { this.secretKey = value; return this; }
		public Builder timeoutMillis(Long value) { this.timeoutMillis = value; return this; }
		public Builder configService(ConfigService value) { this.configService = value; return this; }

		public NacosPublisherConfig build() {
			return new NacosPublisherConfig(this);
		}
	}
}
