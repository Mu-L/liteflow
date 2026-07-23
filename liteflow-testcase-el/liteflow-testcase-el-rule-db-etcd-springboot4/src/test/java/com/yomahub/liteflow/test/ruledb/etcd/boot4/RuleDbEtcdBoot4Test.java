package com.yomahub.liteflow.test.ruledb.etcd.boot4;

import com.yomahub.liteflow.property.LiteflowConfigGetter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(classes = RuleDbEtcdBoot4Application.class)
public class RuleDbEtcdBoot4Test {

	@Test
	public void boot4BindsEtcdConfigurationWithoutOpeningClient() {
		Assertions.assertEquals("ruledb-etcd-boot4-it",
				LiteflowConfigGetter.get().getRuleDb().getApplicationName());
		Assertions.assertEquals("/liteflow-boot4-it",
				LiteflowConfigGetter.get().getRuleDb().getEtcd().getRootPath());
	}
}
