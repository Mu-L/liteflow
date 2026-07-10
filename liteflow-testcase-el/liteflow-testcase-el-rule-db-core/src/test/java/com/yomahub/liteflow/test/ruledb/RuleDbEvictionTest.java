package com.yomahub.liteflow.test.ruledb;

import com.yomahub.liteflow.core.FlowExecutor;
import com.yomahub.liteflow.flow.FlowBus;
import com.yomahub.liteflow.flow.element.Chain;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.repository.RuleDbCache;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RuleDbEvictionTest extends BaseRuleDbTest {

	@Test
	public void testChainEvictedFallsBackToShadowAndReloadable() {
		for (int i = 1; i <= 6; i++) {
			InMemoryRuleRepository.putChain("chain" + i, "THEN(a, b)");
		}
		registerCommonCmp();
		RuleDbConfig cfg = new RuleDbConfig();
		cfg.setCacheCapacity(2); // 小容量强制淘汰
		FlowExecutor executor = buildExecutor(cfg);

		for (int i = 1; i <= 6; i++) {
			Assertions.assertTrue(executor.execute2Resp("chain" + i, "arg").isSuccess());
		}
		// 强制 Caffeine 清理，使淘汰监听器同步执行
		RuleDbCache.cleanUp();

		// 至少有 chain 退回影子（未编译）
		int shadow = 0;
		for (Chain c : FlowBus.getChainMap().values()) {
			if (!c.isCompiled()) {
				shadow++;
			}
		}
		Assertions.assertTrue(shadow > 0, "expected some chains evicted to shadow state");

		// 被淘汰的 chain 仍可重新执行（重新回源编译）
		Assertions.assertTrue(executor.execute2Resp("chain1", "arg").isSuccess());
	}

	@Test
	public void testScriptRefCountUnloadOnEviction() {
		InMemoryRuleRepository.putChain("cs1", "THEN(a, sx)");
		InMemoryRuleRepository.putChain("cs2", "THEN(b, sx)");
		InMemoryRuleRepository.putScript("sx", "defaultContext.setData(\"sx\", true);", "script", "groovy");
		registerCommonCmp();
		RuleDbConfig cfg = new RuleDbConfig();
		cfg.setCacheCapacity(1); // 一次只容一个 chain
		FlowExecutor executor = buildExecutor(cfg);

		executor.execute2Resp("cs1", "arg");
		executor.execute2Resp("cs2", "arg");
		RuleDbCache.cleanUp();

		// 两个 chain 引用同一脚本，容量 1 时最终至多一个 chain 驻留，引用计数 <= 1
		Assertions.assertTrue(RuleDbCache.scriptRefCount("sx") <= 1);
	}
}
