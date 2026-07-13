package com.yomahub.liteflow.repository.redis;

import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.property.RuleDbRedisConfig;
import com.yomahub.liteflow.spi.holder.ContextAwareHolder;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.ClusterServersConfig;
import org.redisson.config.Config;
import org.redisson.config.SentinelServersConfig;
import org.redisson.config.SingleServerConfig;

/** Resolves one execution-provider or publisher Redis client. */
public class RedisConnectionManager {

	private final RuleDbRedisConfig executionConfig;
	private final RedisPublisherConfig publisherConfig;
	private final boolean dynamicExecutionConfig;

	private volatile RedissonClient client;
	private volatile boolean selfCreated;

	public RedisConnectionManager() {
		this.executionConfig = null;
		this.publisherConfig = null;
		this.dynamicExecutionConfig = true;
	}

	RedisConnectionManager(RuleDbRedisConfig config) {
		this.executionConfig = config;
		this.publisherConfig = null;
		this.dynamicExecutionConfig = false;
	}

	RedisConnectionManager(RedisPublisherConfig config) {
		this.executionConfig = null;
		this.publisherConfig = config;
		this.dynamicExecutionConfig = false;
		this.client = config.getRedissonClient();
	}

	public RedissonClient getClient() {
		if (client != null) {
			return client;
		}
		synchronized (this) {
			if (client != null) {
				return client;
			}
			RedissonClient bean = publisherConfig == null ? lookupBean(execution()) : null;
			if (bean != null) {
				client = bean;
				return client;
			}
			if (StrUtil.isBlank(address())) {
				throw new ConfigErrorException(
						"rule-db redis: neither a RedissonClient nor liteflow.rule-db.redis.address is available");
			}
			client = Redisson.create(buildConfig());
			selfCreated = true;
			return client;
		}
	}

	public synchronized void shutdown() {
		if (client != null && selfCreated) {
			client.shutdown();
		}
		client = null;
		selfCreated = false;
	}

	private RedissonClient lookupBean(RuleDbRedisConfig config) {
		try {
			if (StrUtil.isNotBlank(config.getRedissonBeanName())) {
				return ContextAwareHolder.loadContextAware().getBean(config.getRedissonBeanName());
			}
			return ContextAwareHolder.loadContextAware().getBean(RedissonClient.class);
		}
		catch (Exception e) {
			return null;
		}
	}

	private Config buildConfig() {
		Config config = new Config();
		String[] addresses = address().split(",");
		int database = database() == null ? 0 : database();
		if (addresses.length > 1 && StrUtil.isNotBlank(masterName())) {
			SentinelServersConfig sentinel = config.useSentinelServers()
					.setMasterName(masterName())
					.setDatabase(database);
			for (String item : addresses) {
				sentinel.addSentinelAddress(normalize(item));
			}
			applyCredentials(sentinel);
		}
		else if (addresses.length > 1) {
			ClusterServersConfig cluster = config.useClusterServers();
			for (String item : addresses) {
				cluster.addNodeAddress(normalize(item));
			}
			applyCredentials(cluster);
		}
		else {
			SingleServerConfig single = config.useSingleServer()
					.setAddress(normalize(addresses[0]))
					.setDatabase(database);
			applyCredentials(single);
		}
		return config;
	}

	private void applyCredentials(SentinelServersConfig config) {
		if (StrUtil.isNotBlank(password())) {
			config.setPassword(password());
		}
		if (StrUtil.isNotBlank(username())) {
			config.setUsername(username());
		}
	}

	private void applyCredentials(ClusterServersConfig config) {
		if (StrUtil.isNotBlank(password())) {
			config.setPassword(password());
		}
		if (StrUtil.isNotBlank(username())) {
			config.setUsername(username());
		}
	}

	private void applyCredentials(SingleServerConfig config) {
		if (StrUtil.isNotBlank(password())) {
			config.setPassword(password());
		}
		if (StrUtil.isNotBlank(username())) {
			config.setUsername(username());
		}
	}

	private String address() {
		return publisherConfig == null ? execution().getAddress() : publisherConfig.getAddress();
	}

	private String masterName() {
		return publisherConfig == null ? execution().getMasterName() : publisherConfig.getMasterName();
	}

	private String username() {
		return publisherConfig == null ? execution().getUsername() : publisherConfig.getUsername();
	}

	private String password() {
		return publisherConfig == null ? execution().getPassword() : publisherConfig.getPassword();
	}

	private Integer database() {
		return publisherConfig == null ? execution().getDatabase() : publisherConfig.getDatabase();
	}

	private RuleDbRedisConfig execution() {
		if (!dynamicExecutionConfig) {
			return executionConfig;
		}
		RuleDbConfig config = LiteflowConfigGetter.get().getRuleDb();
		return config == null || config.getRedis() == null ? new RuleDbRedisConfig() : config.getRedis();
	}

	private String normalize(String address) {
		String value = address.trim();
		return value.startsWith("redis://") || value.startsWith("rediss://")
				? value : "redis://" + value;
	}
}
