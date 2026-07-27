package com.yomahub.liteflow.property;

/** Redis Rule-DB execution configuration. */
public class RuleDbRedisConfig {

	private String address;
	private String masterName;
	private String username;
	private String password;
	private Integer database = 0;
	private String keyPrefix = "lf";
	private String keyHashTag;
	private String redissonBeanName;

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

	public String getKeyHashTag() {
		return keyHashTag;
	}

	public void setKeyHashTag(String keyHashTag) {
		this.keyHashTag = keyHashTag;
	}

	public String getRedissonBeanName() {
		return redissonBeanName;
	}

	public void setRedissonBeanName(String redissonBeanName) {
		this.redissonBeanName = redissonBeanName;
	}
}
