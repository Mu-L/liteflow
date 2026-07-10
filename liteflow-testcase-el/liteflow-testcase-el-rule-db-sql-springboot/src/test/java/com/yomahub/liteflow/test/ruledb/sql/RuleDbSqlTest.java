/**
 * <p>Title: liteflow</p>
 * <p>Description: 轻量级的组件式流程框架</p>
 */
package com.yomahub.liteflow.test.ruledb.sql;

import com.yomahub.liteflow.core.FlowExecutor;
import com.yomahub.liteflow.flow.LiteflowResponse;
import com.yomahub.liteflow.repository.RuleDbSyncManager;
import com.yomahub.liteflow.repository.sql.SqlRulePublisher;
import com.yomahub.liteflow.repository.vo.ScriptRecord;
import com.yomahub.liteflow.slot.DefaultContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Rule-DB SQL 插件端到端集成测试：发布到 H2 → FlowExecutor 执行（惰性回源 H2）→
 * 发布新版本 → seq 轮询收敛到新逻辑。
 *
 * <p>类名以 {@code Test} 结尾以匹配 maven-surefire-plugin 的默认 includes
 * （本仓库未配置 failsafe，{@code *IT} 不会被 surefire 收集）。
 *
 * @author Bryan.Zhang
 * @since 2.16.2
 */
@SpringBootTest(classes = RuleDbSqlApplication.class)
public class RuleDbSqlTest {

	@Autowired
	private FlowExecutor flowExecutor;

	@Test
	public void testPublishThenExecuteAndConverge() {
		SqlRulePublisher publisher = new SqlRulePublisher();
		publisher.publishChain("chainA", "THEN(a, b)");
		// 触发一次对账，把新发布的 chain 纳入索引（显式对账更稳，不依赖启动时 manifest）
		RuleDbSyncManager.reconcileOnce();

		LiteflowResponse r1 = flowExecutor.execute2Resp("chainA", "arg");
		Assertions.assertTrue(r1.isSuccess());
		Assertions.assertEquals("a==>b", r1.getExecuteStepStr());

		// 发布新版本（执行顺序反转），通过 seq 轮询收敛
		publisher.publishChain("chainA", "THEN(b, a)");
		RuleDbSyncManager.pollOnce();

		LiteflowResponse r2 = flowExecutor.execute2Resp("chainA", "arg");
		Assertions.assertTrue(r2.isSuccess());
		Assertions.assertEquals("b==>a", r2.getExecuteStepStr());
	}

	@Test
	public void testScriptPublishAndExecute() {
		SqlRulePublisher publisher = new SqlRulePublisher();
		ScriptRecord s = new ScriptRecord();
		s.setNodeId("sqlS1");
		s.setType("script");
		s.setLanguage("groovy");
		s.setScript("defaultContext.setData(\"sqlS1\", true);");
		publisher.publishScript(s);
		publisher.publishChain("chainS", "THEN(a, sqlS1)");
		RuleDbSyncManager.reconcileOnce();

		LiteflowResponse r = flowExecutor.execute2Resp("chainS", "arg");
		Assertions.assertTrue(r.isSuccess());
		Assertions.assertEquals(Boolean.TRUE,
				r.getContextBean(DefaultContext.class).getData("sqlS1"));
	}

}
