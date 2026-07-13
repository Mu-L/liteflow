package com.yomahub.liteflow.test.ruledb;

import com.yomahub.liteflow.property.RuleDbConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class RuleDbConfigTest {

	@Test
	void exposesGroupedDefaults() {
		RuleDbConfig config = new RuleDbConfig();

		Assertions.assertTrue(config.getEnabled());
		Assertions.assertEquals(500, config.getCache().getCapacity());
		Assertions.assertEquals(60, config.getSync().getReconcileSeconds());
		Assertions.assertEquals(3, config.getSync().getFetchRetryTimes());
		Assertions.assertNull(config.getSync().getPollSeconds());
		Assertions.assertEquals("lf_", config.getSql().getTablePrefix());
		Assertions.assertFalse(config.getSql().getAutoInitTable());
		Assertions.assertEquals(0, config.getRedis().getDatabase());
		Assertions.assertEquals("lf", config.getRedis().getKeyPrefix());
		Assertions.assertEquals("/liteflow", config.getEtcd().getRootPath());
		Assertions.assertEquals("/liteflow", config.getZk().getRootPath());
	}
}
