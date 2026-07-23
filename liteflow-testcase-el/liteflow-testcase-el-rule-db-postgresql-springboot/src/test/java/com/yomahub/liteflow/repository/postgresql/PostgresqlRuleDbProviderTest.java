package com.yomahub.liteflow.repository.postgresql;

import com.yomahub.liteflow.property.LiteflowConfig;
import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.repository.ChangeSourceHealth;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class PostgresqlRuleDbProviderTest {

	@AfterEach
	void resetConfig() {
		LiteflowConfigGetter.clean();
	}

	@Test
	void providerWiresRepositoryAndPollingChangeSourceFromConfig() {
		LiteflowConfig liteflowConfig = new LiteflowConfig();
		RuleDbConfig ruleDb = new RuleDbConfig();
		ruleDb.setApplicationName("wired-app");
		ruleDb.getSync().setPollSeconds(60);
		ruleDb.getPostgresql().setChangeLogBatchSize(50);
		liteflowConfig.setRuleDb(ruleDb);
		LiteflowConfigGetter.setLiteflowConfig(liteflowConfig);

		PostgresqlRuleDbProvider provider = new PostgresqlRuleDbProvider();
		assertEquals("postgresql", provider.type());
		assertNotNull(provider.repository());
		assertNotNull(provider.changeSource());
		assertSame(provider.changeSource(), provider.changeSource());
		assertEquals(ChangeSourceHealth.Status.STARTING, provider.changeSource().health().getStatus());

		provider.close();
		assertEquals(ChangeSourceHealth.Status.DOWN, provider.changeSource().health().getStatus());
	}

	@Test
	void providerFallsBackToDefaultsWhenRuleDbConfigIsAbsent() {
		LiteflowConfigGetter.setLiteflowConfig(new LiteflowConfig());

		PostgresqlRuleDbProvider provider = new PostgresqlRuleDbProvider();
		assertEquals("postgresql", provider.type());
		assertNotNull(provider.repository());
		provider.close();
	}
}
