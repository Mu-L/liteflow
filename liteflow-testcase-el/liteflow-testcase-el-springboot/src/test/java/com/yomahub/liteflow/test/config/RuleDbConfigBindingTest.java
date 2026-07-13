package com.yomahub.liteflow.test.config;

import com.yomahub.liteflow.property.LiteflowConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

@TestPropertySource(properties = {
		"spring.application.name=rule-db-binding",
		"liteflow.rule-db.cache.capacity=123",
		"liteflow.rule-db.cache.preload-chain-ids=c1,c2",
		"liteflow.rule-db.sync.poll-seconds=5",
		"liteflow.rule-db.sync.reconcile-seconds=17",
		"liteflow.rule-db.sql.table-prefix=sql_",
		"liteflow.rule-db.redis.key-prefix=redis",
		"liteflow.rule-db.etcd.root-path=/etcd-rules",
		"liteflow.rule-db.zk.root-path=/zk-rules"
})
@SpringBootTest(classes = RuleDbConfigBindingTest.class)
@EnableAutoConfiguration
class RuleDbConfigBindingTest {

	@Autowired
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
