package com.yomahub.liteflow.test.ruledb;

import com.yomahub.liteflow.core.FlowExecutor;
import com.yomahub.liteflow.exception.ChainLoadException;
import com.yomahub.liteflow.flow.FlowBus;
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

	@Test
	public void testSubChainLazyLoadedRecursively() {
		InMemoryRuleRepository.putChain("chainParent", "THEN(a, sub1)");
		InMemoryRuleRepository.putChain("sub1", "THEN(b)");
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());
		Assertions.assertEquals(0, InMemoryRuleRepository.FETCH_CHAIN_COUNT.get());

		// 执行父链：父链回源编译时发现引用子链，子链沿同一懒加载路径回源（spec §8.2）
		LiteflowResponse r = executor.execute2Resp("chainParent", "arg");
		Assertions.assertTrue(r.isSuccess());
		Assertions.assertEquals("a==>b", r.getExecuteStepStr());
		Assertions.assertEquals(2, InMemoryRuleRepository.FETCH_CHAIN_COUNT.get());

		// 二次执行零回源
		Assertions.assertTrue(executor.execute2Resp("chainParent", "arg").isSuccess());
		Assertions.assertEquals(2, InMemoryRuleRepository.FETCH_CHAIN_COUNT.get());
	}

	@Test
	public void testPreloadCompilesAtStartup() {
		InMemoryRuleRepository.putChain("chainP", "THEN(a, b)");
		InMemoryRuleRepository.putChain("chainLazy", "THEN(b, a)");
		registerCommonCmp();
		RuleDbConfig cfg = new RuleDbConfig();
		cfg.setPreloadChainIds("chainP");
		FlowExecutor executor = buildExecutor(cfg);

		// 预热清单中的 chain 启动即回源编译，其余保持影子
		Assertions.assertEquals(1, InMemoryRuleRepository.FETCH_CHAIN_COUNT.get());
		Assertions.assertTrue(FlowBus.getChain("chainP").isCompiled());
		Assertions.assertFalse(FlowBus.getChain("chainLazy").isCompiled());

		// 预热过的 chain 执行零回源
		Assertions.assertTrue(executor.execute2Resp("chainP", "arg").isSuccess());
		Assertions.assertEquals(1, InMemoryRuleRepository.FETCH_CHAIN_COUNT.get());
	}

	@Test
	public void testFetchRetryTimesRespectedWhenStorageDown() {
		InMemoryRuleRepository.putChain("chain1", "THEN(a, b)");
		registerCommonCmp();
		RuleDbConfig cfg = new RuleDbConfig();
		cfg.setFetchRetryTimes(1);
		FlowExecutor executor = buildExecutor(cfg);

		InMemoryRuleRepository.DOWN = true;
		LiteflowResponse r = executor.execute2Resp("chain1", "arg");
		Assertions.assertFalse(r.isSuccess());
		// 候选加载先读 metadata；fetch-retry-times=1 → 首次 + 重试一次 = 2 次尝试
		Assertions.assertEquals(2, InMemoryRuleRepository.FETCH_CHAIN_META_COUNT.get());
		Assertions.assertEquals(0, InMemoryRuleRepository.FETCH_CHAIN_COUNT.get());
	}

	@Test
	public void testDisabledChainFailsWithLoadException() {
		InMemoryRuleRepository.publishChain("chainD", "THEN(a, b)");
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());

		// 启动后被运营台直接停用（enable=0，无变更记录）：懒加载时按停用语义失败
		InMemoryRuleRepository.disableChain("chainD");
		LiteflowResponse r = executor.execute2Resp("chainD", "arg");
		Assertions.assertFalse(r.isSuccess());
		boolean directChainLoad = r.getCause() instanceof ChainLoadException;
		boolean wrappedChainLoad = r.getCause() != null && r.getCause().getCause() instanceof ChainLoadException;
		Assertions.assertTrue(directChainLoad || wrappedChainLoad,
				"disabled chain should fail with ChainLoadException, actual: "
						+ (r.getCause() == null ? "null" : r.getCause().getClass().getName()));
	}

	@Test
	public void testNamespaceFromRecordApplied() {
		InMemoryRuleRepository.putChain("chainNs", "THEN(a, b)", "ns1");
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());

		Assertions.assertTrue(executor.execute2Resp("chainNs", "arg").isSuccess());
		Assertions.assertEquals("ns1", FlowBus.getChain("chainNs").getNamespace());
	}
}
