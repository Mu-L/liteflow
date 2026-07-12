package com.yomahub.liteflow.test.ruledb;

import com.yomahub.liteflow.core.FlowExecutor;
import com.yomahub.liteflow.flow.FlowBus;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.repository.RuleDbProviderHolder;
import com.yomahub.liteflow.repository.RuleDbRuntime;
import com.yomahub.liteflow.repository.RuleDbSyncManager;
import com.yomahub.liteflow.repository.runtime.RuleTargetState;
import com.yomahub.liteflow.repository.runtime.RuleTargetStatus;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RuleDbVersionStateTest extends BaseRuleDbTest {

	@Test
	public void testManifestCreatesDesiredShadowState() {
		InMemoryRuleRepository.putChain("chain1", "THEN(a, b)");
		buildExecutor(new RuleDbConfig());

		RuleTargetState state = RuleDbRuntime.chainState("chain1");
		Assertions.assertEquals(1L, state.getDesiredVersion());
		Assertions.assertEquals(InMemoryRuleRepository.CHAINS.get("chain1").getMd5(), state.getDesiredMd5());
		Assertions.assertEquals(0L, state.getActiveVersion());
		Assertions.assertEquals(RuleTargetStatus.SHADOW, state.getStatus());
	}

	@Test
	public void testFirstSuccessfulLoadActivatesDesiredVersion() {
		FlowExecutor executor = loadAndExecuteVersionOne();
		Assertions.assertNotNull(executor);

		RuleTargetState state = RuleDbRuntime.chainState("chain1");
		Assertions.assertEquals(1L, state.getDesiredVersion());
		Assertions.assertEquals(1L, state.getActiveVersion());
		Assertions.assertEquals(state.getDesiredMd5(), state.getActiveMd5());
		Assertions.assertEquals(RuleTargetStatus.READY, state.getStatus());
	}

	@Test
	public void testNewChainEventFetchesOnlyMetadata() {
		buildExecutor(new RuleDbConfig());
		InMemoryRuleRepository.publishChain("chain9", "THEN(a, b)");
		emitLastChange();

		Assertions.assertEquals(1, InMemoryRuleRepository.FETCH_CHAIN_META_COUNT.get());
		Assertions.assertEquals(0, InMemoryRuleRepository.FETCH_CHAIN_COUNT.get());
		Assertions.assertEquals(RuleTargetStatus.SHADOW, RuleDbRuntime.chainState("chain9").getStatus());
	}

	@Test
	public void testNewScriptEventFetchesOnlyMetadata() {
		buildExecutor(new RuleDbConfig());
		InMemoryRuleRepository.publishScript("s9", "println('x')", "script", "groovy");
		emitLastChange();

		Assertions.assertEquals(1, InMemoryRuleRepository.FETCH_SCRIPT_META_COUNT.get());
		Assertions.assertEquals(0, InMemoryRuleRepository.FETCH_SCRIPT_COUNT.get());
		Assertions.assertEquals(RuleTargetStatus.SHADOW, RuleDbRuntime.scriptState("s9").getStatus());
	}

	@Test
	public void testUpsertChangesDesiredButKeepsActive() {
		loadAndExecuteVersionOne();
		InMemoryRuleRepository.publishChain("chain1", "THEN(b, a)");
		emitLastChange();

		RuleTargetState state = RuleDbRuntime.chainState("chain1");
		Assertions.assertEquals(2L, state.getDesiredVersion());
		Assertions.assertEquals(1L, state.getActiveVersion());
		Assertions.assertEquals(RuleTargetStatus.STALE, state.getStatus());
		Assertions.assertEquals("THEN(a, b)", FlowBus.getChain("chain1").getEl());
	}

	@Test
	public void testEqualVersionEventKeepsReadyState() {
		loadAndExecuteVersionOne();
		int fetched = InMemoryRuleRepository.FETCH_CHAIN_COUNT.get();
		provider().emit(new ChangeRecord(2, ChangeRecord.TargetType.CHAIN,
				"chain1", ChangeRecord.Op.UPSERT, 1));

		RuleTargetState state = RuleDbRuntime.chainState("chain1");
		Assertions.assertEquals(1L, state.getDesiredVersion());
		Assertions.assertEquals(1L, state.getActiveVersion());
		Assertions.assertEquals(RuleTargetStatus.READY, state.getStatus());
		Assertions.assertEquals(fetched, InMemoryRuleRepository.FETCH_CHAIN_COUNT.get());
	}

	@Test
	public void testReconcileSameVersionMd5UpdatesDesiredOnly() {
		loadAndExecuteVersionOne();
		RuleTargetState state = RuleDbRuntime.chainState("chain1");
		String activeMd5 = state.getActiveMd5();
		InMemoryRuleRepository.dirtyWriteChainSameVersion("chain1", "THEN(b, a)");

		RuleDbSyncManager.reconcileOnce();

		Assertions.assertEquals(1L, state.getDesiredVersion());
		Assertions.assertEquals(1L, state.getActiveVersion());
		Assertions.assertNotEquals(activeMd5, state.getDesiredMd5());
		Assertions.assertEquals(activeMd5, state.getActiveMd5());
		Assertions.assertEquals(RuleTargetStatus.STALE, state.getStatus());
	}

	@Test
	public void testDeleteMarksStateAndRemovesOwnedShadow() {
		loadAndExecuteVersionOne();
		InMemoryRuleRepository.deleteChain("chain1");
		emitLastChange();

		RuleTargetState state = RuleDbRuntime.chainState("chain1");
		Assertions.assertEquals(1L, state.getActiveVersion());
		Assertions.assertEquals(RuleTargetStatus.DELETED, state.getStatus());
		Assertions.assertNull(RuleDbRuntime.getChainVersion("chain1"));
		Assertions.assertFalse(FlowBus.containChain("chain1"));
	}

	private FlowExecutor loadAndExecuteVersionOne() {
		InMemoryRuleRepository.publishChain("chain1", "THEN(a, b)");
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());
		Assertions.assertTrue(executor.execute2Resp("chain1", "arg").isSuccess());
		return executor;
	}

	private void emitLastChange() {
		provider().emit(InMemoryRuleRepository.CHANGES.get(InMemoryRuleRepository.CHANGES.size() - 1));
	}

	private InMemoryRuleDbProvider provider() {
		return (InMemoryRuleDbProvider) RuleDbProviderHolder.get();
	}
}
