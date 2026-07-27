package com.yomahub.liteflow.test.ruledb.zk.boot4;

import com.yomahub.liteflow.property.LiteflowConfigGetter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(classes = RuleDbZkBoot4Application.class)
public class RuleDbZkBoot4Test {

	@Test
	public void boot4BindsZkConfigurationWithoutOpeningClient() {
		Assertions.assertEquals("ruledb-zk-boot4-it",
				LiteflowConfigGetter.get().getRuleDb().getApplicationName());
		Assertions.assertEquals(6000,
				LiteflowConfigGetter.get().getRuleDb().getZk().getSessionTimeout());
	}
}
