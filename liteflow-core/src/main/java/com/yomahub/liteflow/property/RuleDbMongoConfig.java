package com.yomahub.liteflow.property;

/** MongoDB Rule-DB execution configuration. */
public class RuleDbMongoConfig {

	private String uri;
	private String database = "liteflow";
	private String collectionPrefix = "lf_";
	private String mongoClientBeanName;
	private Integer changeLogBatchSize = 1000;

	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public String getCollectionPrefix() {
		return collectionPrefix;
	}

	public void setCollectionPrefix(String collectionPrefix) {
		this.collectionPrefix = collectionPrefix;
	}

	public String getMongoClientBeanName() {
		return mongoClientBeanName;
	}

	public void setMongoClientBeanName(String mongoClientBeanName) {
		this.mongoClientBeanName = mongoClientBeanName;
	}

	public Integer getChangeLogBatchSize() {
		return changeLogBatchSize;
	}

	public void setChangeLogBatchSize(Integer changeLogBatchSize) {
		this.changeLogBatchSize = changeLogBatchSize;
	}
}
