package com.yomahub.liteflow.test.ruledb;

import com.yomahub.liteflow.core.FlowExecutor;
import com.yomahub.liteflow.flow.FlowBus;
import com.yomahub.liteflow.flow.LiteflowResponse;
import com.yomahub.liteflow.flow.element.Node;
import com.yomahub.liteflow.meta.LiteflowMetaOperator;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.repository.RuleDbRuntime;
import com.yomahub.liteflow.repository.RuleDbSyncManager;
import com.yomahub.liteflow.repository.runtime.RuleTargetStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * DELETE 变更收敛：chain / script 删除经轮询路径落到本节点后，
 * FlowBus 条目移除、后续执行按"不存在"语义失败，且不中断同批其他变更。
 */
public class RuleDbDeleteTest extends BaseRuleDbTest {

	@Test
	public void testDeleteLoadedChainViaPoll() {
		InMemoryRuleRepository.publishChain("chainDel", "THEN(a, b)");
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());
		Assertions.assertTrue(executor.execute2Resp("chainDel", "arg").isSuccess()); // 已加载（有 elMd5）

		InMemoryRuleRepository.deleteChain("chainDel");
		RuleDbSyncManager.pollOnce();

		Assertions.assertFalse(FlowBus.containChain("chainDel"));
		LiteflowResponse r = executor.execute2Resp("chainDel", "arg");
		Assertions.assertFalse(r.isSuccess(), "deleted chain must not execute");
	}

	@Test
	public void testDeleteScriptThenReferencingChainFailsOnRecompile() {
		InMemoryRuleRepository.publishScript("s1", "defaultContext.setData(\"s1\", true);", "script", "groovy");
		InMemoryRuleRepository.publishChain("chainS", "THEN(a, s1)");
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());
		Assertions.assertTrue(executor.execute2Resp("chainS", "arg").isSuccess());

		// 删除脚本，但 chain 新版仍然引用它：重编译时节点缺失，执行必须失败而不是静默跑旧逻辑
		InMemoryRuleRepository.deleteScript("s1");
		InMemoryRuleRepository.publishChain("chainS", "THEN(b, s1)");
		RuleDbSyncManager.pollOnce();

		Assertions.assertNull(FlowBus.getNode("s1"), "deleted script node should be removed from nodeMap");
		LiteflowResponse r = executor.execute2Resp("chainS", "arg");
		Assertions.assertFalse(r.isSuccess(), "chain referencing a deleted script must fail on recompile");
	}

	@Test
	public void testDeleteScriptInvalidatesCompiledChainNode() {
		InMemoryRuleRepository.publishScript("s1", "defaultContext.setData(\"s1\", true);", "script", "groovy");
		InMemoryRuleRepository.publishChain("chainS", "THEN(a, s1)");
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());
		Assertions.assertTrue(executor.execute2Resp("chainS", "arg").isSuccess());

		InMemoryRuleRepository.deleteScript("s1");
		RuleDbSyncManager.pollOnce();

		Assertions.assertNull(FlowBus.getNode("s1"));
		Assertions.assertFalse(executor.execute2Resp("chainS", "arg").isSuccess());
	}

	@Test
	public void testDeleteScriptWhileChainDropsReference() {
		InMemoryRuleRepository.publishScript("s1", "defaultContext.setData(\"s1\", true);", "script", "groovy");
		InMemoryRuleRepository.publishChain("chainS", "THEN(a, s1)");
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());
		Assertions.assertTrue(executor.execute2Resp("chainS", "arg").isSuccess());

		// 正确的删除姿势：chain 新版先摘掉引用，再删脚本，同批收敛后照常执行
		InMemoryRuleRepository.publishChain("chainS", "THEN(a, b)");
		InMemoryRuleRepository.deleteScript("s1");
		RuleDbSyncManager.pollOnce();

		LiteflowResponse r = executor.execute2Resp("chainS", "arg");
		Assertions.assertTrue(r.isSuccess());
		Assertions.assertEquals("a==>b", r.getExecuteStepStr());
		Assertions.assertNull(FlowBus.getNode("s1"));
	}

	@Test
	public void testDeleteUnknownChainKeepsBatchGoing() {
		InMemoryRuleRepository.publishChain("chain1", "THEN(a, b)");
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());
		Assertions.assertTrue(executor.execute2Resp("chain1", "arg").isSuccess());

		// storage 里从来没有 ghost 这个 chain；DELETE 落空不应中断同批后续变更
		InMemoryRuleRepository.deleteChain("ghost");
		InMemoryRuleRepository.publishChain("chain1", "THEN(b, a)");
		Assertions.assertDoesNotThrow(RuleDbSyncManager::pollOnce);

		LiteflowResponse r = executor.execute2Resp("chain1", "arg");
		Assertions.assertTrue(r.isSuccess());
		Assertions.assertEquals("b==>a", r.getExecuteStepStr());
	}

	@Test
	public void testDeleteScriptSkipsNestedShadowChainAndInvalidatesCompiledClone() {
		InMemoryRuleRepository.publishScript("safeDeleteScript", "defaultContext.setData(\"deleted\", true);", "script", "groovy");
		InMemoryRuleRepository.publishChain("shadowChain", "THEN(a, b)");
		InMemoryRuleRepository.publishChain("compiledChain", "THEN(a, safeDeleteScript, shadowChain)");
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());
		Assertions.assertTrue(executor.execute2Resp("compiledChain", "arg").isSuccess());
		Node compiledClone = LiteflowMetaOperator.getNodes("compiledChain").stream()
				.filter(node -> "safeDeleteScript".equals(node.getId()))
				.findFirst()
				.orElseThrow(() -> new AssertionError("compiled script clone not found"));
		FlowBus.getChain("shadowChain").setConditionList(null);
		Assertions.assertNull(FlowBus.getChain("shadowChain").getConditionList());

		InMemoryRuleRepository.deleteScript("safeDeleteScript");

		Assertions.assertDoesNotThrow(RuleDbSyncManager::pollOnce);
		Assertions.assertEquals(RuleTargetStatus.DELETED,
				RuleDbRuntime.scriptState("safeDeleteScript").getStatus());
		Assertions.assertNull(compiledClone.getScript());
		Assertions.assertFalse(compiledClone.isCompiled());
		Assertions.assertNull(FlowBus.getNode("safeDeleteScript"));
		Assertions.assertFalse(executor.execute2Resp("compiledChain", "arg").isSuccess());
	}

}
