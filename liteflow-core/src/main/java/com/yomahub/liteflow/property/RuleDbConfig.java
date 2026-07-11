package com.yomahub.liteflow.property;

/**
 * Rule-DB 模式统一配置（liteflow.rule-db.*），SQL/Redis 字段取并集，插件各取所需
 *
 * @author Bryan.Zhang
 * @since 2.16.1
 */
public class RuleDbConfig {

	private Boolean enabled = Boolean.TRUE;

	private String applicationName;

	private Integer cacheCapacity = 500;

	// null=插件默认（SQL:3 / Redis:30）
	private Integer seqPollSeconds;

	private Integer reconcileSeconds = 60;

	// 逗号分隔
	private String preloadChainIds;

	private Integer fetchRetryTimes = 3;

	// ---- SQL ----
	private String url;

	private String username;

	private String password;

	// 空则由 url 推断
	private String driverClassName;

	// 空则自动查找容器 DataSource
	private String datasourceBeanName;

	private String tablePrefix = "lf_";

	private Boolean autoInitTable = Boolean.FALSE;

	// ---- Redis ----
	// 多地址逗号分隔
	private String address;

	// 配置即哨兵模式
	private String masterName;

	private Integer database = 0;

	private String keyPrefix = "lf";

	private String redissonBeanName;

	public Boolean getEnabled() {
		return enabled;
	}

	public void setEnabled(Boolean enabled) {
		this.enabled = enabled;
	}

	public String getApplicationName() {
		return applicationName;
	}

	public void setApplicationName(String applicationName) {
		this.applicationName = applicationName;
	}

	public Integer getCacheCapacity() {
		return cacheCapacity;
	}

	public void setCacheCapacity(Integer cacheCapacity) {
		this.cacheCapacity = cacheCapacity;
	}

	public Integer getSeqPollSeconds() {
		return seqPollSeconds;
	}

	public void setSeqPollSeconds(Integer seqPollSeconds) {
		this.seqPollSeconds = seqPollSeconds;
	}

	public Integer getReconcileSeconds() {
		return reconcileSeconds;
	}

	public void setReconcileSeconds(Integer reconcileSeconds) {
		this.reconcileSeconds = reconcileSeconds;
	}

	public String getPreloadChainIds() {
		return preloadChainIds;
	}

	public void setPreloadChainIds(String preloadChainIds) {
		this.preloadChainIds = preloadChainIds;
	}

	public Integer getFetchRetryTimes() {
		return fetchRetryTimes;
	}

	public void setFetchRetryTimes(Integer fetchRetryTimes) {
		this.fetchRetryTimes = fetchRetryTimes;
	}

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

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public String getMasterName() {
		return masterName;
	}

	public void setMasterName(String masterName) {
		this.masterName = masterName;
	}

	public Integer getDatabase() {
		return database;
	}

	public void setDatabase(Integer database) {
		this.database = database;
	}

	public String getKeyPrefix() {
		return keyPrefix;
	}

	public void setKeyPrefix(String keyPrefix) {
		this.keyPrefix = keyPrefix;
	}

	public String getRedissonBeanName() {
		return redissonBeanName;
	}

	public void setRedissonBeanName(String redissonBeanName) {
		this.redissonBeanName = redissonBeanName;
	}

}
