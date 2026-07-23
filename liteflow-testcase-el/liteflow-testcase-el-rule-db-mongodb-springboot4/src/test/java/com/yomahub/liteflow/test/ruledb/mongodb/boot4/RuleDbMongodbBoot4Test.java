package com.yomahub.liteflow.test.ruledb.mongodb.boot4;

import com.yomahub.liteflow.property.LiteflowConfigGetter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(classes = RuleDbMongodbBoot4Application.class)
public class RuleDbMongodbBoot4Test {

	@Test
	public void boot4BindsMongodbConfigurationWithoutOpeningClient() {
		Assertions.assertEquals("ruledb-mongodb-boot4-it",
				LiteflowConfigGetter.get().getRuleDb().getApplicationName());
		Assertions.assertEquals(17,
				LiteflowConfigGetter.get().getRuleDb().getMongodb().getChangeLogBatchSize());
	}
}
