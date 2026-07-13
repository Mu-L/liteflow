package com.yomahub.liteflow.repository.redis;

import com.yomahub.liteflow.publisher.PublisherBackend;
import com.yomahub.liteflow.publisher.RulePublisherConfig;
import org.redisson.api.RedissonClient;

/** Type-safe connection configuration for an independent Redis publisher. */
public final class RedisPublisherConfig implements RulePublisherConfig {

	private final String applicationName;
	private final String address;
	private final String masterName;
	private final String username;
	private final String password;
	private final Integer database;
	private final String keyPrefix;
	private final RedissonClient redissonClient;

	private RedisPublisherConfig(Builder builder) {
		this.applicationName = builder.applicationName;
		this.address = builder.address;
		this.masterName = builder.masterName;
		this.username = builder.username;
		this.password = builder.password;
		this.database = builder.database;
		this.keyPrefix = builder.keyPrefix;
		this.redissonClient = builder.redissonClient;
	}

	public static Builder builder() {
		return new Builder();
	}

	@Override
	public String applicationName() {
		return applicationName;
	}

	@Override
	public PublisherBackend backend() {
		return PublisherBackend.REDIS;
	}

	public String getAddress() {
		return address;
	}

	public String getMasterName() {
		return masterName;
	}

	public String getUsername() {
		return username;
	}

	public String getPassword() {
		return password;
	}

	public Integer getDatabase() {
		return database;
	}

	public String getKeyPrefix() {
		return keyPrefix;
	}

	public RedissonClient getRedissonClient() {
		return redissonClient;
	}

	public static final class Builder {

		private String applicationName;
		private String address;
		private String masterName;
		private String username;
		private String password;
		private Integer database = 0;
		private String keyPrefix = "lf";
		private RedissonClient redissonClient;

		private Builder() {
		}

		public Builder applicationName(String applicationName) {
			this.applicationName = applicationName;
			return this;
		}

		public Builder address(String address) {
			this.address = address;
			return this;
		}

		public Builder masterName(String masterName) {
			this.masterName = masterName;
			return this;
		}

		public Builder username(String username) {
			this.username = username;
			return this;
		}

		public Builder password(String password) {
			this.password = password;
			return this;
		}

		public Builder database(Integer database) {
			this.database = database;
			return this;
		}

		public Builder keyPrefix(String keyPrefix) {
			this.keyPrefix = keyPrefix;
			return this;
		}

		public Builder redissonClient(RedissonClient redissonClient) {
			this.redissonClient = redissonClient;
			return this;
		}

		public RedisPublisherConfig build() {
			return new RedisPublisherConfig(this);
		}
	}
}
