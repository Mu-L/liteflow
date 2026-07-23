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
 * @since 2.16.1
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
	public void testRemoveChainConvergesToNotFound() {
		SqlRulePublisher publisher = new SqlRulePublisher();
		publisher.publishChain("delChain", "THEN(a, b)");
		RuleDbSyncManager.reconcileOnce();
		Assertions.assertTrue(flowExecutor.execute2Resp("delChain", "arg").isSuccess());

		publisher.removeChain("delChain");
		RuleDbSyncManager.pollOnce();

		LiteflowResponse r = flowExecutor.execute2Resp("delChain", "arg");
		Assertions.assertFalse(r.isSuccess(), "removed chain must not execute after convergence");
	}

	@Test
	public void testChainOnlyUpdateKeepsScriptExecutable() {
		SqlRulePublisher publisher = new SqlRulePublisher();
		ScriptRecord s = new ScriptRecord();
		s.setNodeId("hotS1");
		s.setType("script");
		s.setLanguage("groovy");
		s.setScript("defaultContext.setData(\"hotS1\", true);");
		publisher.publishScript(s);
		publisher.publishChain("hotChain", "THEN(a, hotS1)");
		RuleDbSyncManager.reconcileOnce();
		Assertions.assertTrue(flowExecutor.execute2Resp("hotChain", "arg").isSuccess());

		// 仅更新 chain（脚本不变），重编后脚本节点必须仍可执行：
		// 重编时脚本未失效不会走 compileScriptNode 回源，若缓存登记把引用计数瞬时归零
		// 误卸载执行器里的脚本，这里会报 script for node[hotS1] is not loaded
		publisher.publishChain("hotChain", "THEN(hotS1, a)");
		RuleDbSyncManager.pollOnce();

		LiteflowResponse r = flowExecutor.execute2Resp("hotChain", "arg");
		Assertions.assertTrue(r.isSuccess(), "script node must stay executable after chain-only update, but got: "
				+ (r.getCause() == null ? "" : r.getCause().getMessage()));
		Assertions.assertEquals("hotS1==>a", r.getExecuteStepStr());
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
