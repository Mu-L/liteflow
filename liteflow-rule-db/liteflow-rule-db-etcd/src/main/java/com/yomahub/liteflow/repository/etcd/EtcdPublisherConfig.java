package com.yomahub.liteflow.repository.etcd;

import com.yomahub.liteflow.publisher.PublisherBackend;
import com.yomahub.liteflow.publisher.RulePublisherConfig;
import io.etcd.jetcd.Client;

public final class EtcdPublisherConfig implements RulePublisherConfig {

	private final String applicationName;
	private final String endpoints;
	private final String user;
	private final String password;
	private final String rootPath;
	private final String caCertificate;
	private final String clientCertificate;
	private final String clientKey;
	private final String authority;
	private final Long connectTimeoutMillis;
	private final Long keepaliveTimeSeconds;
	private final Long keepaliveTimeoutSeconds;
	private final Boolean keepaliveWithoutCalls;
	private final Client client;

	private EtcdPublisherConfig(Builder builder) {
		this.applicationName = builder.applicationName;
		this.endpoints = builder.endpoints;
		this.user = builder.user;
		this.password = builder.password;
		this.rootPath = builder.rootPath;
		this.caCertificate = builder.caCertificate;
		this.clientCertificate = builder.clientCertificate;
		this.clientKey = builder.clientKey;
		this.authority = builder.authority;
		this.connectTimeoutMillis = builder.connectTimeoutMillis;
		this.keepaliveTimeSeconds = builder.keepaliveTimeSeconds;
		this.keepaliveTimeoutSeconds = builder.keepaliveTimeoutSeconds;
		this.keepaliveWithoutCalls = builder.keepaliveWithoutCalls;
		this.client = builder.client;
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
		return PublisherBackend.ETCD;
	}

	public String getEndpoints() { return endpoints; }
	public String getUser() { return user; }
	public String getPassword() { return password; }
	public String getRootPath() { return rootPath; }
	public String getCaCertificate() { return caCertificate; }
	public String getClientCertificate() { return clientCertificate; }
	public String getClientKey() { return clientKey; }
	public String getAuthority() { return authority; }
	public Long getConnectTimeoutMillis() { return connectTimeoutMillis; }
	public Long getKeepaliveTimeSeconds() { return keepaliveTimeSeconds; }
	public Long getKeepaliveTimeoutSeconds() { return keepaliveTimeoutSeconds; }
	public Boolean getKeepaliveWithoutCalls() { return keepaliveWithoutCalls; }
	public Client getClient() { return client; }

	public static final class Builder {
		private String applicationName;
		private String endpoints;
		private String user;
		private String password;
		private String rootPath = "/liteflow";
		private String caCertificate;
		private String clientCertificate;
		private String clientKey;
		private String authority;
		private Long connectTimeoutMillis = 5000L;
		private Long keepaliveTimeSeconds = 30L;
		private Long keepaliveTimeoutSeconds = 10L;
		private Boolean keepaliveWithoutCalls = true;
		private Client client;

		private Builder() { }

		public Builder applicationName(String value) { this.applicationName = value; return this; }
		public Builder endpoints(String value) { this.endpoints = value; return this; }
		public Builder user(String value) { this.user = value; return this; }
		public Builder password(String value) { this.password = value; return this; }
		public Builder rootPath(String value) { this.rootPath = value; return this; }
		public Builder caCertificate(String value) { this.caCertificate = value; return this; }
		public Builder clientCertificate(String value) { this.clientCertificate = value; return this; }
		public Builder clientKey(String value) { this.clientKey = value; return this; }
		public Builder authority(String value) { this.authority = value; return this; }
		public Builder connectTimeoutMillis(Long value) { this.connectTimeoutMillis = value; return this; }
		public Builder keepaliveTimeSeconds(Long value) { this.keepaliveTimeSeconds = value; return this; }
		public Builder keepaliveTimeoutSeconds(Long value) { this.keepaliveTimeoutSeconds = value; return this; }
		public Builder keepaliveWithoutCalls(Boolean value) { this.keepaliveWithoutCalls = value; return this; }
		public Builder client(Client value) { this.client = value; return this; }
		public EtcdPublisherConfig build() { return new EtcdPublisherConfig(this); }
	}
}
