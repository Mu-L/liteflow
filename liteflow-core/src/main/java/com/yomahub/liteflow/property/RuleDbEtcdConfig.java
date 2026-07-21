package com.yomahub.liteflow.property;

/** etcd Rule-DB execution configuration. */
public class RuleDbEtcdConfig {

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
	private String clientBeanName;

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

	public String getCaCertificate() {
		return caCertificate;
	}

	public void setCaCertificate(String caCertificate) {
		this.caCertificate = caCertificate;
	}

	public String getClientCertificate() {
		return clientCertificate;
	}

	public void setClientCertificate(String clientCertificate) {
		this.clientCertificate = clientCertificate;
	}

	public String getClientKey() {
		return clientKey;
	}

	public void setClientKey(String clientKey) {
		this.clientKey = clientKey;
	}

	public String getAuthority() {
		return authority;
	}

	public void setAuthority(String authority) {
		this.authority = authority;
	}

	public Long getConnectTimeoutMillis() {
		return connectTimeoutMillis;
	}

	public void setConnectTimeoutMillis(Long connectTimeoutMillis) {
		this.connectTimeoutMillis = connectTimeoutMillis;
	}

	public Long getKeepaliveTimeSeconds() {
		return keepaliveTimeSeconds;
	}

	public void setKeepaliveTimeSeconds(Long keepaliveTimeSeconds) {
		this.keepaliveTimeSeconds = keepaliveTimeSeconds;
	}

	public Long getKeepaliveTimeoutSeconds() {
		return keepaliveTimeoutSeconds;
	}

	public void setKeepaliveTimeoutSeconds(Long keepaliveTimeoutSeconds) {
		this.keepaliveTimeoutSeconds = keepaliveTimeoutSeconds;
	}

	public Boolean getKeepaliveWithoutCalls() {
		return keepaliveWithoutCalls;
	}

	public void setKeepaliveWithoutCalls(Boolean keepaliveWithoutCalls) {
		this.keepaliveWithoutCalls = keepaliveWithoutCalls;
	}

	public String getClientBeanName() {
		return clientBeanName;
	}

	public void setClientBeanName(String clientBeanName) {
		this.clientBeanName = clientBeanName;
	}
}
