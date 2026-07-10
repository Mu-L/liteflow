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
		// 两个 chain 各自独占一个脚本：cs1→sx1, cs2→sx2
		// cacheCapacity=1 时 cs2 驻留必然淘汰 cs1，其独占脚本 sx1 引用计数归零被 unload
		InMemoryRuleRepository.putChain("cs1", "THEN(a, sx1)");
		InMemoryRuleRepository.putChain("cs2", "THEN(b, sx2)");
		InMemoryRuleRepository.putScript("sx1", "defaultContext.setData(\"sx1\", true);", "script", "groovy");
		InMemoryRuleRepository.putScript("sx2", "defaultContext.setData(\"sx2\", true);", "script", "groovy");
		registerCommonCmp();
		RuleDbConfig cfg = new RuleDbConfig();
		cfg.setCacheCapacity(1); // 一次只容一个 chain
		FlowExecutor executor = buildExecutor(cfg);

		executor.execute2Resp("cs1", "arg");
		executor.execute2Resp("cs2", "arg");
		RuleDbCache.cleanUp();

		// cs1 被淘汰，其独占脚本 sx1 引用计数归零并被 unload
		Assertions.assertEquals(0, RuleDbCache.scriptRefCount("sx1"),
				"evicted chain's exclusive script should have refcount 0 (unloaded)");
		// cs2 仍驻留，sx2 引用计数为 1
		Assertions.assertEquals(1, RuleDbCache.scriptRefCount("sx2"));
	}
}
