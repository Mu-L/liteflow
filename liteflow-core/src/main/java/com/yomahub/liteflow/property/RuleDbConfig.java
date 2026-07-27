package com.yomahub.liteflow.property;

/**
 * Rule-DB execution configuration grouped by responsibility and backend.
 *
 * @author Bryan.Zhang
 * @since 2.16.1
 */
public class RuleDbConfig {

	private Boolean enabled = Boolean.TRUE;

	private String applicationName;

	private RuleDbCacheConfig cache = new RuleDbCacheConfig();

	private RuleDbSyncConfig sync = new RuleDbSyncConfig();

	private RuleDbSqlConfig sql = new RuleDbSqlConfig();

	private RuleDbPostgresqlConfig postgresql = new RuleDbPostgresqlConfig();

	private RuleDbMongoConfig mongodb = new RuleDbMongoConfig();

	private RuleDbRedisConfig redis = new RuleDbRedisConfig();

	private RuleDbEtcdConfig etcd = new RuleDbEtcdConfig();

	private RuleDbZkConfig zk = new RuleDbZkConfig();

	private RuleDbNacosConfig nacos = new RuleDbNacosConfig();

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

	public RuleDbCacheConfig getCache() {
		return cache;
	}

	public void setCache(RuleDbCacheConfig cache) {
		this.cache = cache;
	}

	public RuleDbSyncConfig getSync() {
		return sync;
	}

	public void setSync(RuleDbSyncConfig sync) {
		this.sync = sync;
	}

	public RuleDbSqlConfig getSql() {
		return sql;
	}

	public void setSql(RuleDbSqlConfig sql) {
		this.sql = sql;
	}

	public RuleDbPostgresqlConfig getPostgresql() {
		return postgresql;
	}

	public void setPostgresql(RuleDbPostgresqlConfig postgresql) {
		this.postgresql = postgresql;
	}

	public RuleDbMongoConfig getMongodb() {
		return mongodb;
	}

	public void setMongodb(RuleDbMongoConfig mongodb) {
		this.mongodb = mongodb;
	}

	public RuleDbRedisConfig getRedis() {
		return redis;
	}

	public void setRedis(RuleDbRedisConfig redis) {
		this.redis = redis;
	}

	public RuleDbEtcdConfig getEtcd() {
		return etcd;
	}

	public void setEtcd(RuleDbEtcdConfig etcd) {
		this.etcd = etcd;
	}

	public RuleDbZkConfig getZk() {
		return zk;
	}

	public void setZk(RuleDbZkConfig zk) {
		this.zk = zk;
	}

	public RuleDbNacosConfig getNacos() {
		return nacos;
	}

	public void setNacos(RuleDbNacosConfig nacos) {
		this.nacos = nacos;
	}
}
