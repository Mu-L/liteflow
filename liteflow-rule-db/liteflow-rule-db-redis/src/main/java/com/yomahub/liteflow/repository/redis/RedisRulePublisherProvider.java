package com.yomahub.liteflow.repository.redis;

import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.publisher.PublisherBackend;
import com.yomahub.liteflow.publisher.RulePublisher;
import com.yomahub.liteflow.publisher.RulePublisherConfig;
import com.yomahub.liteflow.publisher.RulePublisherProvider;
import com.yomahub.liteflow.publisher.exception.PublisherConfigurationException;

/** Service provider for Redis-backed rule publishers. */
public class RedisRulePublisherProvider implements RulePublisherProvider {

	@Override
	public boolean supports(RulePublisherConfig config) {
		return config instanceof RedisPublisherConfig && config.backend() == PublisherBackend.REDIS;
	}

	@Override
	public RulePublisher create(RulePublisherConfig config) {
		if (!(config instanceof RedisPublisherConfig)) {
			throw new PublisherConfigurationException("Redis publisher requires RedisPublisherConfig");
		}
		RedisPublisherConfig redis = (RedisPublisherConfig) config;
		if (redis.getRedissonClient() == null && StrUtil.isBlank(redis.getAddress())) {
			throw new PublisherConfigurationException("Redis publisher requires a RedissonClient or address");
		}
		if (StrUtil.isBlank(redis.getKeyPrefix()) || redis.getKeyPrefix().contains(" ")) {
			throw new PublisherConfigurationException("Redis publisher keyPrefix must not be blank or contain spaces");
		}
		return new RedisRulePublisherImpl(redis);
	}
}
