package com.yomahub.liteflow.property;

/** Nacos Rule-DB execution configuration. */
public class RuleDbNacosConfig {

	private String serverAddr;
	private String namespace;
	private String group = "LITEFLOW_RULE_DB";
	private String dataIdPrefix = "liteflow-rule-db";
	private String username;
	private String password;
	private String accessKey;
	private String secretKey;
	private Long timeoutMillis = 3000L;
	private String configServiceBeanName;

	public String getServerAddr() {
		return serverAddr;
	}

	public void setServerAddr(String serverAddr) {
		this.serverAddr = serverAddr;
	}

	public String getNamespace() {
		return namespace;
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public String getDataIdPrefix() {
		return dataIdPrefix;
	}

	public void setDataIdPrefix(String dataIdPrefix) {
		this.dataIdPrefix = dataIdPrefix;
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

	public String getAccessKey() {
		return accessKey;
	}

	public void setAccessKey(String accessKey) {
		this.accessKey = accessKey;
	}

	public String getSecretKey() {
		return secretKey;
	}

	public void setSecretKey(String secretKey) {
		this.secretKey = secretKey;
	}

	public Long getTimeoutMillis() {
		return timeoutMillis;
	}

	public void setTimeoutMillis(Long timeoutMillis) {
		this.timeoutMillis = timeoutMillis;
	}

	public String getConfigServiceBeanName() {
		return configServiceBeanName;
	}

	public void setConfigServiceBeanName(String configServiceBeanName) {
		this.configServiceBeanName = configServiceBeanName;
	}
}
