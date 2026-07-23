package com.yomahub.liteflow.test.ruledb.postgresql.boot4;

import com.yomahub.liteflow.property.LiteflowConfigGetter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(classes = RuleDbPostgresqlBoot4Application.class)
public class RuleDbPostgresqlBoot4Test {

	@Test
	public void boot4BindsPostgresqlConfigurationWithoutOpeningJdbc() {
		Assertions.assertEquals("ruledb-postgresql-boot4-it",
				LiteflowConfigGetter.get().getRuleDb().getApplicationName());
		Assertions.assertEquals(17,
				LiteflowConfigGetter.get().getRuleDb().getPostgresql().getChangeLogBatchSize());
	}
}
