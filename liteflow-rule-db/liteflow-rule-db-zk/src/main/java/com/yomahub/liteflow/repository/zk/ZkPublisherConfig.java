package com.yomahub.liteflow.repository.zk;

import com.yomahub.liteflow.publisher.PublisherBackend;
import com.yomahub.liteflow.publisher.RulePublisherConfig;
import org.apache.curator.framework.CuratorFramework;

public final class ZkPublisherConfig implements RulePublisherConfig {

	private final String applicationName;
	private final String connectString;
	private final Integer sessionTimeout;
	private final String rootPath;
	private final CuratorFramework client;

	private ZkPublisherConfig(Builder builder) {
		this.applicationName = builder.applicationName;
		this.connectString = builder.connectString;
		this.sessionTimeout = builder.sessionTimeout;
		this.rootPath = builder.rootPath;
		this.client = builder.client;
	}

	public static Builder builder() { return new Builder(); }

	@Override public String applicationName() { return applicationName; }
	@Override public PublisherBackend backend() { return PublisherBackend.ZK; }
	public String getConnectString() { return connectString; }
	public Integer getSessionTimeout() { return sessionTimeout; }
	public String getRootPath() { return rootPath; }
	public CuratorFramework getClient() { return client; }

	public static final class Builder {
		private String applicationName;
		private String connectString;
		private Integer sessionTimeout = 60000;
		private String rootPath = "/liteflow";
		private CuratorFramework client;
		private Builder() { }
		public Builder applicationName(String value) { this.applicationName = value; return this; }
		public Builder connectString(String value) { this.connectString = value; return this; }
		public Builder sessionTimeout(Integer value) { this.sessionTimeout = value; return this; }
		public Builder rootPath(String value) { this.rootPath = value; return this; }
		public Builder client(CuratorFramework value) { this.client = value; return this; }
		public ZkPublisherConfig build() { return new ZkPublisherConfig(this); }
	}
}
