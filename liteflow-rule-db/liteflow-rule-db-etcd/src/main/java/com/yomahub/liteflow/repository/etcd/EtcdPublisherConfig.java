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
	private final Client client;

	private EtcdPublisherConfig(Builder builder) {
		this.applicationName = builder.applicationName;
		this.endpoints = builder.endpoints;
		this.user = builder.user;
		this.password = builder.password;
		this.rootPath = builder.rootPath;
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
	public Client getClient() { return client; }

	public static final class Builder {
		private String applicationName;
		private String endpoints;
		private String user;
		private String password;
		private String rootPath = "/liteflow";
		private Client client;

		private Builder() { }

		public Builder applicationName(String value) { this.applicationName = value; return this; }
		public Builder endpoints(String value) { this.endpoints = value; return this; }
		public Builder user(String value) { this.user = value; return this; }
		public Builder password(String value) { this.password = value; return this; }
		public Builder rootPath(String value) { this.rootPath = value; return this; }
		public Builder client(Client value) { this.client = value; return this; }
		public EtcdPublisherConfig build() { return new EtcdPublisherConfig(this); }
	}
}
