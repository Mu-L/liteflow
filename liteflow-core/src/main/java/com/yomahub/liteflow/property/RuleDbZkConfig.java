package com.yomahub.liteflow.property;

/** ZooKeeper Rule-DB execution configuration. */
public class RuleDbZkConfig {

	private String connectString;
	private Integer sessionTimeout;
	private String rootPath = "/liteflow";
	private String username;
	private String password;
	private String curatorBeanName;

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

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getCuratorBeanName() {
		return curatorBeanName;
	}

	public void setCuratorBeanName(String curatorBeanName) {
		this.curatorBeanName = curatorBeanName;
	}
}
