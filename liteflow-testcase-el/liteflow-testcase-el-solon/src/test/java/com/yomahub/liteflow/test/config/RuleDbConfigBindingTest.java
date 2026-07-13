package com.yomahub.liteflow.test.config;

import com.yomahub.liteflow.property.LiteflowConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.noear.solon.annotation.Import;
import org.noear.solon.annotation.Inject;
import org.noear.solon.test.SolonTest;

@SolonTest
@Import(profiles = "classpath:/ruleDbConfig/application.properties")
public class RuleDbConfigBindingTest {

	@Inject
	private LiteflowConfig liteflowConfig;

	@Test
	void bindsGroupedRuleDbConfiguration() {
		Assertions.assertEquals("rule-db-binding", liteflowConfig.getRuleDb().getApplicationName());
		Assertions.assertEquals(123, liteflowConfig.getRuleDb().getCache().getCapacity());
		Assertions.assertEquals("c1,c2", liteflowConfig.getRuleDb().getCache().getPreloadChainIds());
		Assertions.assertEquals(5, liteflowConfig.getRuleDb().getSync().getPollSeconds());
		Assertions.assertEquals(17, liteflowConfig.getRuleDb().getSync().getReconcileSeconds());
		Assertions.assertEquals("sql_", liteflowConfig.getRuleDb().getSql().getTablePrefix());
		Assertions.assertEquals("redis", liteflowConfig.getRuleDb().getRedis().getKeyPrefix());
		Assertions.assertEquals("/etcd-rules", liteflowConfig.getRuleDb().getEtcd().getRootPath());
		Assertions.assertEquals("/zk-rules", liteflowConfig.getRuleDb().getZk().getRootPath());
	}
}
