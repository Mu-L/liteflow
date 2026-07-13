package com.yomahub.liteflow.repository.redis;

import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.property.RuleDbRedisConfig;
import com.yomahub.liteflow.repository.RuleChangeSource;
import com.yomahub.liteflow.repository.RuleDbProvider;
import com.yomahub.liteflow.repository.RuleRepository;

/** Redis execution provider using the same polling lifecycle as SQL. */
public class RedisRuleDbProvider implements RuleDbProvider {

	private final RedisRuleRepository repository;
	private final RedisPollingChangeSource changeSource;

	public RedisRuleDbProvider() {
		RuleDbConfig config = LiteflowConfigGetter.get().getRuleDb();
		RuleDbRedisConfig redis = config == null || config.getRedis() == null
				? new RuleDbRedisConfig() : config.getRedis();
		String applicationName = config == null ? null : config.getApplicationName();
		if (StrUtil.isBlank(applicationName)) {
			applicationName = "default";
		}
		RedisConnectionManager connectionManager = new RedisConnectionManager(redis);
		RedisKeys keys = new RedisKeys(redis.getKeyPrefix(), applicationName);
		this.repository = new RedisRuleRepository(connectionManager, keys);
		int pollSeconds = config == null || config.getSync() == null
				|| config.getSync().getPollSeconds() == null ? 3 : config.getSync().getPollSeconds();
		this.changeSource = new RedisPollingChangeSource(repository, pollSeconds);
	}

	@Override
	public String type() {
		return "redis";
	}

	@Override
	public RuleRepository repository() {
		return repository;
	}

	@Override
	public RuleChangeSource changeSource() {
		return changeSource;
	}

	@Override
	public void close() {
		changeSource.close();
		repository.close();
	}
}
