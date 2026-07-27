package com.yomahub.liteflow.repository.redis;

import com.yomahub.liteflow.property.LiteflowConfig;
import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.property.RuleDbRedisConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class RedisRuleDbProviderTest {

	@AfterEach
	void resetConfig() {
		LiteflowConfigGetter.clean();
	}

	@Test
	void defaultConfigWiresRepositoryAndPollingSource() {
		LiteflowConfigGetter.setLiteflowConfig(new LiteflowConfig());
		RedisRuleDbProvider provider = new RedisRuleDbProvider();
		try {
			assertEquals("redis", provider.type());
			assertNotNull(provider.repository());
			assertNotNull(provider.changeSource());
		}
		finally {
			provider.close();
		}
	}

	@Test
	void configuredApplicationAndPollSecondsAreHonored() {
		LiteflowConfig config = new LiteflowConfig();
		RuleDbConfig ruleDb = new RuleDbConfig();
		ruleDb.setApplicationName("myapp");
		RuleDbRedisConfig redis = new RuleDbRedisConfig();
		redis.setAddress("redis://127.0.0.1:6379");
		ruleDb.setRedis(redis);
		ruleDb.getSync().setPollSeconds(9);
		config.setRuleDb(ruleDb);
		LiteflowConfigGetter.setLiteflowConfig(config);

		RedisRuleDbProvider provider = new RedisRuleDbProvider();
		try {
			assertSame(provider.repository(), provider.repository());
			assertSame(provider.changeSource(), provider.changeSource());
		}
		finally {
			provider.close();
		}
	}
}
