package com.yomahub.liteflow.property;

/** etcd Rule-DB execution configuration. */
public class RuleDbEtcdConfig {

	private String endpoints;
	private String user;
	private String password;
	private String rootPath = "/liteflow";

	public String getEndpoints() {
		return endpoints;
	}

	public void setEndpoints(String endpoints) {
		this.endpoints = endpoints;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getRootPath() {
		return rootPath;
	}

	public void setRootPath(String rootPath) {
		this.rootPath = rootPath;
	}
}
