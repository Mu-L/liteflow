package com.yomahub.liteflow.test.ruledb;

import com.yomahub.liteflow.core.FlowExecutor;
import com.yomahub.liteflow.flow.FlowBus;
import com.yomahub.liteflow.flow.LiteflowResponse;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.repository.RuleDbRuntime;
import com.yomahub.liteflow.repository.RuleDbSyncManager;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import com.yomahub.liteflow.slot.DefaultContext;
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
	public void testScriptChangeConvergesForAllChainsViaPoll() {
		InMemoryRuleRepository.publishScript("s1", "defaultContext.setData(\"val\", \"old\");", "script", "groovy");
		InMemoryRuleRepository.publishChain("chainA", "THEN(a, s1)");
		InMemoryRuleRepository.publishChain("chainB", "THEN(b, s1)");
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());

		// 两条 chain 都编译。chainA 先执行：s1 的克隆经 compileScriptNode 驻留 nodeMap；
		// chainB 编译时克隆的是已编译节点（浅拷贝连 isCompiled/instance 一起拷），不再走编译钩子
		Assertions.assertEquals("old",
				executor.execute2Resp("chainA", "arg").getContextBean(DefaultContext.class).getData("val"));
		Assertions.assertEquals("old",
				executor.execute2Resp("chainB", "arg").getContextBean(DefaultContext.class).getData("val"));

		// 只发布脚本新版，chain 不动。失效必须覆盖所有条件树里的克隆：
		// 若只失效 nodeMap 驻留的克隆（chainA 的），chainB 的克隆仍是已编译态，
		// 在别的 chain 重载执行器产物之前会一直跑旧脚本
		InMemoryRuleRepository.publishScript("s1", "defaultContext.setData(\"val\", \"new\");", "script", "groovy");
		RuleDbSyncManager.pollOnce();

		LiteflowResponse rB = executor.execute2Resp("chainB", "arg");
		Assertions.assertTrue(rB.isSuccess());
		Assertions.assertEquals("new", rB.getContextBean(DefaultContext.class).getData("val"));
		LiteflowResponse rA = executor.execute2Resp("chainA", "arg");
		Assertions.assertTrue(rA.isSuccess());
		Assertions.assertEquals("new", rA.getContextBean(DefaultContext.class).getData("val"));
	}

	@Test
	public void testNewScriptViaPollIsExecutable() {
		InMemoryRuleRepository.publishChain("chain1", "THEN(a, b)");
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());
		executor.execute2Resp("chain1", "arg");

		// 另一节点发布新脚本 + 引用它的 chain 新版，仅经轮询路径感知（不做全量对账）：
		// 新脚本必须注册影子 Node，否则 chain 在下次对账前都编译不过
		InMemoryRuleRepository.publishScript("s9", "defaultContext.setData(\"s9\", true);", "script", "groovy");
		InMemoryRuleRepository.publishChain("chain1", "THEN(a, s9)");
		RuleDbSyncManager.pollOnce();

		LiteflowResponse r = executor.execute2Resp("chain1", "arg");
		Assertions.assertTrue(r.isSuccess());
		Assertions.assertEquals(Boolean.TRUE, r.getContextBean(DefaultContext.class).getData("s9"));
	}

	@Test
	public void testDeleteShadowChainViaPollKeepsBatchGoing() {
		InMemoryRuleRepository.publishChain("chainShadow", "THEN(a, b)");
		InMemoryRuleRepository.publishChain("chain1", "THEN(a, b)");
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());
		// chainShadow 从不执行，保持影子态（elMd5 为 null）

		// 同一批变更：先删影子 chain，再发布 chain1 新版；DELETE 不能中断本批后续变更
		InMemoryRuleRepository.deleteChain("chainShadow");
		InMemoryRuleRepository.publishChain("chain1", "THEN(b, a)");
		Assertions.assertDoesNotThrow(RuleDbSyncManager::pollOnce);

		Assertions.assertFalse(FlowBus.containChain("chainShadow"));
		LiteflowResponse r = executor.execute2Resp("chain1", "arg");
		Assertions.assertTrue(r.isSuccess());
		Assertions.assertEquals("b==>a", r.getExecuteStepStr());
	}

	@Test
	public void testChainDirtyWriteSameVersionConvergesViaMd5Reconcile() {
		InMemoryRuleRepository.publishChain("chain1", "THEN(a, b)");
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());
		executor.execute2Resp("chain1", "arg");

		// 脏写：内容与 md5 都变了但版本号没动（自算 md5 却忘了 version+1 的后台），
		// 对账的 md5 双保险要能纠正
		InMemoryRuleRepository.dirtyWriteChainSameVersion("chain1", "THEN(b, a)");
		RuleDbSyncManager.reconcileOnce();

		LiteflowResponse r = executor.execute2Resp("chain1", "arg");
		Assertions.assertTrue(r.isSuccess());
		Assertions.assertEquals("b==>a", r.getExecuteStepStr());
	}

	@Test
	public void testScriptDirtyWriteSameVersionConvergesViaMd5Reconcile() {
		InMemoryRuleRepository.publishScript("s1", "defaultContext.setData(\"val\", \"old\");", "script", "groovy");
		InMemoryRuleRepository.publishChain("chain1", "THEN(a, s1)");
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());
		executor.execute2Resp("chain1", "arg");

		InMemoryRuleRepository.dirtyWriteScriptSameVersion("s1", "defaultContext.setData(\"val\", \"new\");");
		RuleDbSyncManager.reconcileOnce();

		LiteflowResponse r = executor.execute2Resp("chain1", "arg");
		Assertions.assertTrue(r.isSuccess());
		Assertions.assertEquals("new", r.getContextBean(DefaultContext.class).getData("val"));
	}

	@Test
	public void testNewChainViaPollIsExecutable() {
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());

		// 启动后另一节点发布全新 chain：轮询路径要注册影子并可执行
		InMemoryRuleRepository.publishChain("chainNew", "THEN(a, b)");
		RuleDbSyncManager.pollOnce();

		LiteflowResponse r = executor.execute2Resp("chainNew", "arg");
		Assertions.assertTrue(r.isSuccess());
		Assertions.assertEquals("a==>b", r.getExecuteStepStr());
	}

	@Test
	public void testStaleChangeReplayKeepsNewContent() {
		InMemoryRuleRepository.publishChain("chain1", "THEN(a, b)");
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());
		executor.execute2Resp("chain1", "arg");

		InMemoryRuleRepository.publishChain("chain1", "THEN(b, a)");
		RuleDbSyncManager.pollOnce();
		Assertions.assertEquals("b==>a", executor.execute2Resp("chain1", "arg").getExecuteStepStr());
		int fetched = InMemoryRuleRepository.FETCH_CHAIN_COUNT.get();

		// 重放过期变更（乱序晚到的旧通知）：既不打回旧版，也不触发无谓失效/回源
		RuleDbRuntime.applyChange(new ChangeRecord(99, ChangeRecord.TargetType.CHAIN,
				"chain1", ChangeRecord.Op.UPSERT, 1));
		Assertions.assertEquals("b==>a", executor.execute2Resp("chain1", "arg").getExecuteStepStr());
		Assertions.assertEquals(fetched, InMemoryRuleRepository.FETCH_CHAIN_COUNT.get());
	}

	@Test
	public void testScriptInvalidationToleratesShadowSubChain() {
		InMemoryRuleRepository.publishScript("s1", "defaultContext.setData(\"val\", \"old\");", "script", "groovy");
		InMemoryRuleRepository.publishChain("sub2", "THEN(b)");
		InMemoryRuleRepository.publishChain("chainTop", "THEN(a, sub2, s1)");
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());
		Assertions.assertTrue(executor.execute2Resp("chainTop", "arg").isSuccess());

		// 子链先被更新失效（退影子，条件树置空），父链仍编译态且树中持有子链引用
		InMemoryRuleRepository.publishChain("sub2", "THEN(c)");
		RuleDbSyncManager.pollOnce();

		// 此时脚本更新到达：失效遍历不能被影子子链绊倒
		InMemoryRuleRepository.publishScript("s1", "defaultContext.setData(\"val\", \"new\");", "script", "groovy");
		Assertions.assertDoesNotThrow(RuleDbSyncManager::pollOnce);

		LiteflowResponse r = executor.execute2Resp("chainTop", "arg");
		Assertions.assertTrue(r.isSuccess());
		Assertions.assertEquals("new", r.getContextBean(DefaultContext.class).getData("val"));
		Assertions.assertEquals("a==>c==>s1", r.getExecuteStepStr());
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
