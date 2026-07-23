package com.yomahub.liteflow.test.ruledb.redis.boot4;

import com.yomahub.liteflow.property.LiteflowConfigGetter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(classes = RuleDbRedisBoot4Application.class)
public class RuleDbRedisBoot4Test {

	@Test
	public void boot4BindsRedisConfigurationWithoutOpeningConnection() {
		Assertions.assertEquals("ruledb-redis-boot4-it",
				LiteflowConfigGetter.get().getRuleDb().getApplicationName());
		Assertions.assertEquals("lfboot4it",
				LiteflowConfigGetter.get().getRuleDb().getRedis().getKeyPrefix());
	}
}
