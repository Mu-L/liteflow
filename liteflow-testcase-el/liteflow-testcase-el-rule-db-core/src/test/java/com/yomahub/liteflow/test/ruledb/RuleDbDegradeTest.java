package com.yomahub.liteflow.test.ruledb;

import com.yomahub.liteflow.core.FlowExecutor;
import com.yomahub.liteflow.exception.ChainLoadException;
import com.yomahub.liteflow.flow.LiteflowResponse;
import com.yomahub.liteflow.property.RuleDbConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Rule-DB 降级与容错行为固化：
 *
 * <ul>
 *   <li>存储不可用 + 缓存命中 → 仍可正常执行（零回源，纯本地缓存兜底）。</li>
 *   <li>存储不可用 + 缓存未命中 → 回源重试耗尽后抛 {@link ChainLoadException}，
 *       response 携带该异常且标记失败。</li>
 * </ul>
 *
 * <p>不新增任何生产代码，仅把已有运行时行为钉死。
 */
public class RuleDbDegradeTest extends BaseRuleDbTest {

	@Test
	public void testCachedChainStillExecutesWhenStorageDown() {
		InMemoryRuleRepository.putChain("chain1", "THEN(a, b)");
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());

		// 先执行一次，进缓存
		Assertions.assertTrue(executor.execute2Resp("chain1", "arg").isSuccess());
		int fetchedBeforeDown = InMemoryRuleRepository.FETCH_CHAIN_COUNT.get();

		// 存储宕机
		InMemoryRuleRepository.DOWN = true;
		// 缓存命中仍可执行（零回源）
		Assertions.assertTrue(executor.execute2Resp("chain1", "arg").isSuccess());
		Assertions.assertEquals(fetchedBeforeDown, InMemoryRuleRepository.FETCH_CHAIN_COUNT.get());
	}

	@Test
	public void testUncachedChainFailsWithChainLoadExceptionWhenStorageDown() {
		InMemoryRuleRepository.putChain("chain1", "THEN(a, b)");
		InMemoryRuleRepository.putChain("chain2", "THEN(b, a)");
		registerCommonCmp();
		RuleDbConfig cfg = new RuleDbConfig();
		cfg.setFetchRetryTimes(1);
		FlowExecutor executor = buildExecutor(cfg);
		executor.execute2Resp("chain1", "arg"); // 只缓存 chain1

		InMemoryRuleRepository.DOWN = true;
		// chain2 未缓存，回源失败 → response 携带 ChainLoadException
		LiteflowResponse r = executor.execute2Resp("chain2", "arg");
		Assertions.assertFalse(r.isSuccess());

		// 实测异常类型链：r.getCause() 即为 ChainLoadException（无额外包裹）。
		// buildUnCompileChain → ensureChainLoaded → fetchChainWithRetry 直接抛出，
		// 经 Chain.execute / FlowExecutor.doExecute 的 catch(Exception) 透传到 slot，
		// LiteflowResponse.newMainResponse(slot) 取 slot.getException() 设为 cause。
		// 这里同时兼容"包裹一层"的形态以防未来传播路径调整。
		Assertions.assertNotNull(r.getCause(), "cause should not be null when execution fails");
		boolean directChainLoad = r.getCause() instanceof ChainLoadException;
		boolean wrappedChainLoad = r.getCause().getCause() instanceof ChainLoadException;
		Assertions.assertTrue(directChainLoad || wrappedChainLoad,
				"cause chain should contain ChainLoadException, actual cause: "
						+ (r.getCause() == null ? "null" : r.getCause().getClass().getName()));
	}
}
