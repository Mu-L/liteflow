package com.yomahub.liteflow.test.ruledb;

import com.yomahub.liteflow.core.FlowExecutor;
import com.yomahub.liteflow.flow.FlowBus;
import com.yomahub.liteflow.flow.element.Chain;
import com.yomahub.liteflow.flow.element.Node;
import com.yomahub.liteflow.meta.LiteflowMetaOperator;
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
		Assertions.assertTrue(FlowBus.getChain("chain1").isCompiled());
	}

	@Test
	public void testScriptUpsertKeepsActiveCompiledArtifactUntilExecution() {
		InMemoryRuleRepository.publishScript("s1", "defaultContext.setData(\"s1\", \"old\");", "script", "groovy");
		InMemoryRuleRepository.publishChain("chain1", "THEN(a, s1)");
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());
		Assertions.assertTrue(executor.execute2Resp("chain1", "arg").isSuccess());
		Node activeNode = scriptNode("chain1", "s1");
		String activeScript = activeNode.getScript();

		InMemoryRuleRepository.publishScript("s1", "defaultContext.setData(\"s1\", \"new\");", "script", "groovy");
		emitLastChange();

		Assertions.assertEquals(activeScript, scriptNode("chain1", "s1").getScript());
		Assertions.assertTrue(scriptNode("chain1", "s1").isCompiled());
		Assertions.assertEquals(1L, RuleDbRuntime.scriptState("s1").getActiveVersion());
		Assertions.assertEquals(RuleTargetStatus.STALE, RuleDbRuntime.scriptState("s1").getStatus());
	}

	@Test
	public void testInvalidFirstChainDoesNotBecomeReady() {
		InMemoryRuleRepository.publishChain("badChain", "THEN(missing)");
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());

		Assertions.assertFalse(executor.execute2Resp("badChain", "arg").isSuccess());
		RuleTargetState state = RuleDbRuntime.chainState("badChain");
		Assertions.assertEquals(0L, state.getActiveVersion());
		Assertions.assertEquals(RuleTargetStatus.FAILED, state.getStatus());
		Assertions.assertNotNull(state.getLastError());
	}

	@Test
	public void testInvalidFirstScriptDoesNotBecomeReady() {
		InMemoryRuleRepository.publishScript("badScript", "defaultContext.setData(", "script", "groovy");
		InMemoryRuleRepository.publishChain("badScriptChain", "THEN(a, badScript)");
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());

		Assertions.assertFalse(executor.execute2Resp("badScriptChain", "arg").isSuccess());
		RuleTargetState state = RuleDbRuntime.scriptState("badScript");
		Assertions.assertEquals(0L, state.getActiveVersion());
		Assertions.assertEquals(RuleTargetStatus.FAILED, state.getStatus());
		Assertions.assertNotNull(state.getLastError());
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

	@Test
	public void testDeletedChainRejectsLateCompiledCallback() {
		InMemoryRuleRepository.putChain("lateChain", "THEN(a, b)");
		buildExecutor(new RuleDbConfig());
		RuleTargetState state = RuleDbRuntime.chainState("lateChain");
		Chain loadingChain = FlowBus.getChain("lateChain");
		state.markLoaded(state.getDesiredVersion(), state.getDesiredMd5());

		RuleDbRuntime.applyChange(new ChangeRecord(1, ChangeRecord.TargetType.CHAIN,
				"lateChain", ChangeRecord.Op.DELETE, state.getDesiredVersion()));
		FlowBus.getChainMap().put("lateChain", loadingChain);
		RuleDbRuntime.recordCompiledChain(loadingChain);

		Assertions.assertEquals(RuleTargetStatus.DELETED, state.getStatus());
		Assertions.assertEquals(0L, state.getActiveVersion());
		Assertions.assertFalse(FlowBus.containChain("lateChain"));
	}

	@Test
	public void testDeletedScriptRejectsLateCompiledCallback() {
		InMemoryRuleRepository.putScript("lateScript", "defaultContext.setData(\"late\", true);", "script", "groovy");
		buildExecutor(new RuleDbConfig());
		RuleTargetState state = RuleDbRuntime.scriptState("lateScript");
		Node loadingScript = FlowBus.getNode("lateScript");
		state.markLoaded(state.getDesiredVersion(), state.getDesiredMd5());

		RuleDbRuntime.applyChange(new ChangeRecord(1, ChangeRecord.TargetType.SCRIPT,
				"lateScript", ChangeRecord.Op.DELETE, state.getDesiredVersion()));
		FlowBus.getNodeMap().put("lateScript", loadingScript);
		RuleDbRuntime.recordCompiledScript(loadingScript);

		Assertions.assertEquals(RuleTargetStatus.DELETED, state.getStatus());
		Assertions.assertEquals(0L, state.getActiveVersion());
		Assertions.assertFalse(FlowBus.containNode("lateScript"));
	}

	@Test
	public void testRecreatedChainSurvivesOldCompiledCallback() {
		InMemoryRuleRepository.publishChain("raceChain", "THEN(a, b)");
		buildExecutor(new RuleDbConfig());
		Chain oldChain = FlowBus.getChain("raceChain");
		RuleTargetState oldState = RuleDbRuntime.chainState("raceChain");
		oldState.markLoaded(oldState.getDesiredVersion(), oldState.getDesiredMd5());

		InMemoryRuleRepository.deleteChain("raceChain");
		RuleDbRuntime.applyChange(lastChange());
		InMemoryRuleRepository.publishChain("raceChain", "THEN(b, a)");
		RuleDbRuntime.applyChange(lastChange());
		Chain newShadow = FlowBus.getChain("raceChain");
		RuleTargetState newState = RuleDbRuntime.chainState("raceChain");

		FlowBus.getChainMap().put("raceChain", oldChain);
		RuleDbRuntime.recordCompiledChain(oldChain);

		Assertions.assertSame(newShadow, FlowBus.getChain("raceChain"));
		Assertions.assertEquals(RuleTargetStatus.SHADOW, newState.getStatus());
		Assertions.assertEquals(1L, newState.getDesiredVersion());
		Assertions.assertDoesNotThrow(RuleDbSyncManager::reconcileOnce);
	}

	@Test
	public void testRecreatedScriptSurvivesOldCompiledCallback() {
		InMemoryRuleRepository.publishScript("raceScript", "defaultContext.setData(\"race\", 1);", "script", "groovy");
		buildExecutor(new RuleDbConfig());
		Node oldScript = FlowBus.getNode("raceScript");
		RuleTargetState oldState = RuleDbRuntime.scriptState("raceScript");
		oldState.markLoaded(oldState.getDesiredVersion(), oldState.getDesiredMd5());

		InMemoryRuleRepository.deleteScript("raceScript");
		RuleDbRuntime.applyChange(lastChange());
		InMemoryRuleRepository.publishScript("raceScript", "defaultContext.setData(\"race\", 2);", "script", "groovy");
		RuleDbRuntime.applyChange(lastChange());
		Node newShadow = FlowBus.getNode("raceScript");
		RuleTargetState newState = RuleDbRuntime.scriptState("raceScript");

		FlowBus.getNodeMap().put("raceScript", oldScript);
		RuleDbRuntime.recordCompiledScript(oldScript);

		Assertions.assertSame(newShadow, FlowBus.getNode("raceScript"));
		Assertions.assertEquals(RuleTargetStatus.SHADOW, newState.getStatus());
		Assertions.assertEquals(1L, newState.getDesiredVersion());
		Assertions.assertDoesNotThrow(RuleDbSyncManager::reconcileOnce);
	}

	@Test
	public void testOldChainCallbackDoesNotActivateRecreatedLoadingGeneration() {
		InMemoryRuleRepository.publishChain("loadingChain", "THEN(a, b)");
		buildExecutor(new RuleDbConfig());
		Chain oldChain = FlowBus.getChain("loadingChain");
		RuleTargetState oldState = RuleDbRuntime.chainState("loadingChain");
		oldState.markLoaded(oldState.getDesiredVersion(), oldState.getDesiredMd5());

		InMemoryRuleRepository.deleteChain("loadingChain");
		RuleDbRuntime.applyChange(lastChange());
		InMemoryRuleRepository.publishChain("loadingChain", "THEN(b, a)");
		RuleDbRuntime.applyChange(lastChange());
		Chain newShadow = FlowBus.getChain("loadingChain");
		RuleTargetState newState = RuleDbRuntime.chainState("loadingChain");
		newState.markLoaded(newState.getDesiredVersion(), newState.getDesiredMd5());

		FlowBus.getChainMap().put("loadingChain", oldChain);
		RuleDbRuntime.recordCompiledChain(oldChain);

		Assertions.assertSame(newShadow, FlowBus.getChain("loadingChain"));
		Assertions.assertEquals(0L, newState.getActiveVersion());
		Assertions.assertEquals(RuleTargetStatus.LOADING, newState.getStatus());
	}

	@Test
	public void testOldScriptCallbackDoesNotActivateRecreatedLoadingGeneration() {
		InMemoryRuleRepository.publishScript("loadingScript", "defaultContext.setData(\"loading\", 1);", "script", "groovy");
		buildExecutor(new RuleDbConfig());
		Node oldScript = FlowBus.getNode("loadingScript");
		RuleTargetState oldState = RuleDbRuntime.scriptState("loadingScript");
		oldState.markLoaded(oldState.getDesiredVersion(), oldState.getDesiredMd5());

		InMemoryRuleRepository.deleteScript("loadingScript");
		RuleDbRuntime.applyChange(lastChange());
		InMemoryRuleRepository.publishScript("loadingScript", "defaultContext.setData(\"loading\", 2);", "script", "groovy");
		RuleDbRuntime.applyChange(lastChange());
		Node newShadow = FlowBus.getNode("loadingScript");
		RuleTargetState newState = RuleDbRuntime.scriptState("loadingScript");
		newState.markLoaded(newState.getDesiredVersion(), newState.getDesiredMd5());

		FlowBus.getNodeMap().put("loadingScript", oldScript);
		RuleDbRuntime.recordCompiledScript(oldScript);

		Assertions.assertSame(newShadow, FlowBus.getNode("loadingScript"));
		Assertions.assertEquals(0L, newState.getActiveVersion());
		Assertions.assertEquals(RuleTargetStatus.LOADING, newState.getStatus());
	}

	@Test
	public void testCompiledScriptInstallsFromCanonicalShadow() throws CloneNotSupportedException {
		InMemoryRuleRepository.putScript("installScript", "defaultContext.setData(\"install\", true);", "script", "groovy");
		buildExecutor(new RuleDbConfig());
		Node shadow = FlowBus.getNode("installScript");
		Node compiled = shadow.clone();

		RuleDbRuntime.ensureScriptLoaded(compiled);
		compiled.setCompiled(true);
		RuleDbRuntime.recordCompiledScript(compiled);

		Assertions.assertSame(compiled, FlowBus.getNode("installScript"));
		Assertions.assertEquals(RuleTargetStatus.READY, RuleDbRuntime.scriptState("installScript").getStatus());
	}

	@Test
	public void testOldScriptFailureDoesNotMarkRecreatedState() throws CloneNotSupportedException {
		InMemoryRuleRepository.publishScript("failedScript", "defaultContext.setData(\"failed\", 1);", "script", "groovy");
		buildExecutor(new RuleDbConfig());
		Node loadingScript = FlowBus.getNode("failedScript").clone();
		RuleDbRuntime.ensureScriptLoaded(loadingScript);

		InMemoryRuleRepository.deleteScript("failedScript");
		RuleDbRuntime.applyChange(lastChange());
		InMemoryRuleRepository.publishScript("failedScript", "defaultContext.setData(\"failed\", 2);", "script", "groovy");
		RuleDbRuntime.applyChange(lastChange());
		RuleTargetState newState = RuleDbRuntime.scriptState("failedScript");

		RuleDbRuntime.markScriptLoadFailed(loadingScript, new IllegalStateException("old load failed"));

		Assertions.assertEquals(RuleTargetStatus.SHADOW, newState.getStatus());
		Assertions.assertNull(newState.getLastError());
	}

	@Test
	public void testOldChainFailureDoesNotMarkRecreatedState() {
		InMemoryRuleRepository.publishChain("failedChain", "THEN(a, b)");
		buildExecutor(new RuleDbConfig());
		Chain loadingChain = FlowBus.getChain("failedChain");
		RuleDbRuntime.ensureChainLoaded("failedChain");

		InMemoryRuleRepository.deleteChain("failedChain");
		RuleDbRuntime.applyChange(lastChange());
		InMemoryRuleRepository.publishChain("failedChain", "THEN(b, a)");
		RuleDbRuntime.applyChange(lastChange());
		RuleTargetState newState = RuleDbRuntime.chainState("failedChain");

		RuleDbRuntime.markChainLoadFailed(loadingChain, new IllegalStateException("old load failed"));

		Assertions.assertEquals(RuleTargetStatus.SHADOW, newState.getStatus());
		Assertions.assertNull(newState.getLastError());
	}

	private FlowExecutor loadAndExecuteVersionOne() {
		InMemoryRuleRepository.publishChain("chain1", "THEN(a, b)");
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());
		Assertions.assertTrue(executor.execute2Resp("chain1", "arg").isSuccess());
		return executor;
	}

	private void emitLastChange() {
		provider().emit(lastChange());
	}

	private ChangeRecord lastChange() {
		return InMemoryRuleRepository.CHANGES.get(InMemoryRuleRepository.CHANGES.size() - 1);
	}

	private InMemoryRuleDbProvider provider() {
		return (InMemoryRuleDbProvider) RuleDbProviderHolder.get();
	}

	private Node scriptNode(String chainId, String nodeId) {
		for (Node node : LiteflowMetaOperator.getNodes(chainId)) {
			if (nodeId.equals(node.getId())) {
				return node;
			}
		}
		throw new AssertionError("script node not found: " + nodeId);
	}
}
