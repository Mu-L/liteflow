package com.yomahub.liteflow.test.ruledb;

import com.yomahub.liteflow.core.FlowExecutor;
import com.yomahub.liteflow.flow.LiteflowResponse;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.repository.RuleDbSyncManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Rule-DB 变更收敛测试：验证轮询 / 对账 / seq 断档三路收敛均能让本节点看到最新版规则。
 *
 * <p>测试通过直接调用 {@link RuleDbSyncManager#pollOnce()} / {@link RuleDbSyncManager#reconcileOnce()}
 * 绕过定时，保证确定性（不依赖 wall-clock 等待）。
 */
public class RuleDbConvergeTest extends BaseRuleDbTest {

	@Test
	public void testChangeConvergesViaPoll() {
		InMemoryRuleRepository.publishChain("chain1", "THEN(a, b)");
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());

		LiteflowResponse r1 = executor.execute2Resp("chain1", "arg");
		Assertions.assertTrue(r1.isSuccess());
		Assertions.assertEquals("a==>b", r1.getExecuteStepStr());

		// 另一节点发布新版（记 change_log + 抬 SEQ）
		InMemoryRuleRepository.publishChain("chain1", "THEN(b, a)");
		// 等价于轮询周期到达
		RuleDbSyncManager.pollOnce();

		LiteflowResponse r2 = executor.execute2Resp("chain1", "arg");
		Assertions.assertTrue(r2.isSuccess());
		Assertions.assertEquals("b==>a", r2.getExecuteStepStr());
	}

	@Test
	public void testLostNotificationConvergesViaReconcile() {
		InMemoryRuleRepository.publishChain("chain1", "THEN(a, b)");
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());
		executor.execute2Resp("chain1", "arg");

		// 绕过发布规范直接改内容（丢通知）：putChain 不记 change_log、不抬 SEQ
		InMemoryRuleRepository.putChain("chain1", "THEN(b, a)");
		// 轮询感知不到（SEQ 未变），对账兜底才收敛
		RuleDbSyncManager.pollOnce();
		LiteflowResponse rPoll = executor.execute2Resp("chain1", "arg");
		Assertions.assertTrue(rPoll.isSuccess());
		Assertions.assertEquals("a==>b", rPoll.getExecuteStepStr()); // 仍旧版

		RuleDbSyncManager.reconcileOnce();
		LiteflowResponse rReconcile = executor.execute2Resp("chain1", "arg");
		Assertions.assertTrue(rReconcile.isSuccess());
		Assertions.assertEquals("b==>a", rReconcile.getExecuteStepStr()); // 对账后新版
	}

	@Test
	public void testSeqGapTriggersReconcile() {
		InMemoryRuleRepository.publishChain("chain1", "THEN(a, b)");
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());
		executor.execute2Resp("chain1", "arg");

		InMemoryRuleRepository.publishChain("chain1", "THEN(b, a)");
		// 模拟 change_log 被清理：MIN_SEQ 抬高到当前 seq 之上，制造断档
		InMemoryRuleRepository.MIN_SEQ = InMemoryRuleRepository.SEQ.get() + 1;

		// pollOnce 内部捕获 SeqGapException → 转 reconcileOnce
		RuleDbSyncManager.pollOnce();
		LiteflowResponse r = executor.execute2Resp("chain1", "arg");
		Assertions.assertTrue(r.isSuccess());
		Assertions.assertEquals("b==>a", r.getExecuteStepStr());
	}
}
