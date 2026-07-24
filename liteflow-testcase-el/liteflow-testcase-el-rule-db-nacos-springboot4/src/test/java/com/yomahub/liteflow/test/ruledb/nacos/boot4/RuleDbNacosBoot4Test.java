package com.yomahub.liteflow.test.ruledb.nacos.boot4;

import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbNacosConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(classes = RuleDbNacosBoot4Application.class)
public class RuleDbNacosBoot4Test {

	@Test
	public void boot4BindsNacosConfigurationWithoutOpeningClient() {
		Assertions.assertEquals("ruledb-nacos-boot4-it",
				LiteflowConfigGetter.get().getRuleDb().getApplicationName());
		RuleDbNacosConfig config = LiteflowConfigGetter.get().getRuleDb().getNacos();
		Assertions.assertEquals("127.0.0.1:8848", config.getServerAddr());
		Assertions.assertEquals("dev", config.getNamespace());
		Assertions.assertEquals("RULE_DB_TEST", config.getGroup());
		Assertions.assertEquals("lf-test", config.getDataIdPrefix());
		Assertions.assertEquals(4500L, config.getTimeoutMillis());
		Assertions.assertEquals("borrowedNacosConfigService", config.getConfigServiceBeanName());
	}
}
