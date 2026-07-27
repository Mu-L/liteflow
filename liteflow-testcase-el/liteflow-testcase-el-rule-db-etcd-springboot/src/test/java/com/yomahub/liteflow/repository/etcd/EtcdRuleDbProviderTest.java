package com.yomahub.liteflow.repository.etcd;

import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.property.LiteflowConfig;
import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.property.RuleDbEtcdConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Provider wiring against the global LiteflowConfig, without a live etcd. */
class EtcdRuleDbProviderTest {

	@AfterEach
	void resetConfig() {
		LiteflowConfigGetter.clean();
	}

	@Test
	void failsFastWithoutRuleDbConfiguration() {
		LiteflowConfigGetter.setLiteflowConfig(new LiteflowConfig());
		assertThrows(ConfigErrorException.class, EtcdRuleDbProvider::new);
	}

	@Test
	void buildsRepositoryAndChangeSourceFromConfig() {
		LiteflowConfigGetter.setLiteflowConfig(config(null));
		EtcdRuleDbProvider provider = new EtcdRuleDbProvider();
		try {
			assertEquals("etcd", provider.type());
			assertNotNull(provider.repository());
			assertNotNull(provider.changeSource());
		}
		finally {
			provider.close();
		}
	}

	@Test
	void honorsConfiguredApplicationName() {
		LiteflowConfigGetter.setLiteflowConfig(config("shop"));
		EtcdRuleDbProvider provider = new EtcdRuleDbProvider();
		try {
			assertNotNull(provider.repository());
		}
		finally {
			provider.close();
		}
	}

	private LiteflowConfig config(String applicationName) {
		RuleDbEtcdConfig etcd = new RuleDbEtcdConfig();
		etcd.setEndpoints("http://127.0.0.1:2379");
		RuleDbConfig ruleDb = new RuleDbConfig();
		ruleDb.setEtcd(etcd);
		ruleDb.setApplicationName(applicationName);
		LiteflowConfig config = new LiteflowConfig();
		config.setRuleDb(ruleDb);
		return config;
	}
}
