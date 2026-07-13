package com.yomahub.liteflow.property;

/** ZooKeeper Rule-DB execution configuration. */
public class RuleDbZkConfig {

	private String connectString;
	private Integer sessionTimeout;
	private String rootPath = "/liteflow";

	public String getConnectString() {
		return connectString;
	}

	public void setConnectString(String connectString) {
		this.connectString = connectString;
	}

	public Integer getSessionTimeout() {
		return sessionTimeout;
	}

	public void setSessionTimeout(Integer sessionTimeout) {
		this.sessionTimeout = sessionTimeout;
	}

	public String getRootPath() {
		return rootPath;
	}

	public void setRootPath(String rootPath) {
		this.rootPath = rootPath;
	}
}
