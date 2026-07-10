package com.yomahub.liteflow.test.ruledb;

import com.yomahub.liteflow.core.FlowExecutor;
import com.yomahub.liteflow.flow.LiteflowResponse;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.slot.DefaultContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RuleDbLazyLoadTest extends BaseRuleDbTest {

	@Test
	public void testFirstExecFetchesThenCached() {
		InMemoryRuleRepository.putChain("chain1", "THEN(a, b)");
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());

		// 启动阶段零回源
		Assertions.assertEquals(0, InMemoryRuleRepository.FETCH_CHAIN_COUNT.get());

		// 首次执行触发一次回源
		LiteflowResponse r1 = executor.execute2Resp("chain1", "arg");
		Assertions.assertTrue(r1.isSuccess());
		Assertions.assertEquals(1, InMemoryRuleRepository.FETCH_CHAIN_COUNT.get());

		// 二次执行零回源（命中缓存）
		LiteflowResponse r2 = executor.execute2Resp("chain1", "arg");
		Assertions.assertTrue(r2.isSuccess());
		Assertions.assertEquals(1, InMemoryRuleRepository.FETCH_CHAIN_COUNT.get());
	}

	@Test
	public void testScriptNodeLazyCompile() {
		InMemoryRuleRepository.putChain("chain2", "THEN(a, s1)");
		InMemoryRuleRepository.putScript("s1", "defaultContext.setData(\"s1\", true);", "script", "groovy");
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());

		// 脚本影子已注册但未编译，启动阶段零回源
		Assertions.assertEquals(0, InMemoryRuleRepository.FETCH_SCRIPT_COUNT.get());

		LiteflowResponse r = executor.execute2Resp("chain2", "arg");
		Assertions.assertTrue(r.isSuccess());
		// 首次执行 chain 触发一次 chain 回源；执行到 s1 触发一次 script 回源
		Assertions.assertEquals(1, InMemoryRuleRepository.FETCH_CHAIN_COUNT.get());
		Assertions.assertEquals(1, InMemoryRuleRepository.FETCH_SCRIPT_COUNT.get());
		Assertions.assertEquals(Boolean.TRUE, r.getContextBean(DefaultContext.class).getData("s1"));
	}
}
