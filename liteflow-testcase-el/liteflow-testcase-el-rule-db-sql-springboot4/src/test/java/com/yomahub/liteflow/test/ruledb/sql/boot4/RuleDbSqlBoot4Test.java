package com.yomahub.liteflow.test.ruledb.sql.boot4;

import com.yomahub.liteflow.property.LiteflowConfigGetter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(classes = RuleDbSqlBoot4Application.class)
public class RuleDbSqlBoot4Test {

	@Test
	public void boot4BindsSqlConfigurationWithoutOpeningJdbc() {
		Assertions.assertEquals("ruledb-sql-boot4-it",
				LiteflowConfigGetter.get().getRuleDb().getApplicationName());
		Assertions.assertEquals(17,
				LiteflowConfigGetter.get().getRuleDb().getSql().getChangeLogBatchSize());
	}
}
