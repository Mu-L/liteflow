package com.yomahub.liteflow.property;

/** SQL Rule-DB execution configuration. */
public class RuleDbSqlConfig {

	private String url;
	private String username;
	private String password;
	private String driverClassName;
	private String datasourceBeanName;
	private String tablePrefix = "lf_";
	private Boolean autoInitTable = Boolean.FALSE;

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
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

	public String getDriverClassName() {
		return driverClassName;
	}

	public void setDriverClassName(String driverClassName) {
		this.driverClassName = driverClassName;
	}

	public String getDatasourceBeanName() {
		return datasourceBeanName;
	}

	public void setDatasourceBeanName(String datasourceBeanName) {
		this.datasourceBeanName = datasourceBeanName;
	}

	public String getTablePrefix() {
		return tablePrefix;
	}

	public void setTablePrefix(String tablePrefix) {
		this.tablePrefix = tablePrefix;
	}

	public Boolean getAutoInitTable() {
		return autoInitTable;
	}

	public void setAutoInitTable(Boolean autoInitTable) {
		this.autoInitTable = autoInitTable;
	}
}
