package com.yomahub.liteflow.test.ruledb.sql.boot4;

import com.yomahub.liteflow.core.FlowExecutor;
import com.yomahub.liteflow.flow.LiteflowResponse;
import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.repository.RuleDbSyncManager;
import com.yomahub.liteflow.repository.sql.SqlRulePublisher;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(classes = RuleDbSqlBoot4Application.class)
public class RuleDbSqlBoot4Test {

	@Autowired
	private FlowExecutor flowExecutor;

	@Test
	public void boot4BindsSqlConfigurationAndExecutesPublishedChain() {
		Assertions.assertEquals("ruledb-sql-boot4-it",
				LiteflowConfigGetter.get().getRuleDb().getApplicationName());
		Assertions.assertEquals(17,
				LiteflowConfigGetter.get().getRuleDb().getSql().getChangeLogBatchSize());

		SqlRulePublisher publisher = new SqlRulePublisher();
		publisher.publishChain("boot4Chain", "THEN(a, b)");
		RuleDbSyncManager.reconcileOnce();

		LiteflowResponse response = flowExecutor.execute2Resp("boot4Chain", null);
		Assertions.assertTrue(response.isSuccess(), response.getCause() == null
				? "chain execution failed" : response.getCause().getMessage());
		Assertions.assertEquals("a==>b", response.getExecuteStepStr());
	}
}
