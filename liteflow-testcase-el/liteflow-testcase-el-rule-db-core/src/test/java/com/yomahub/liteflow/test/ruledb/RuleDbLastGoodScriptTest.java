package com.yomahub.liteflow.test.ruledb;

import com.yomahub.liteflow.core.FlowExecutor;
import com.yomahub.liteflow.enums.NodeTypeEnum;
import com.yomahub.liteflow.flow.FlowBus;
import com.yomahub.liteflow.flow.LiteflowResponse;
import com.yomahub.liteflow.flow.element.Node;
import com.yomahub.liteflow.meta.LiteflowMetaOperator;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.repository.RuleDbRuntime;
import com.yomahub.liteflow.repository.RuleDbSyncManager;
import com.yomahub.liteflow.repository.runtime.RuleTargetState;
import com.yomahub.liteflow.repository.runtime.RuleTargetStatus;
import com.yomahub.liteflow.script.ScriptExecutorFactory;
import com.yomahub.liteflow.slot.DefaultContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class RuleDbLastGoodScriptTest extends BaseRuleDbTest {

	private static volatile CountDownLatch oldScriptStarted;
	private static volatile CountDownLatch releaseOldScript;

	@Test
	public void testInvalidVersionTwoKeepsVersionOneExecutable() {
		FlowExecutor executor = loadVersionOne();

		InMemoryRuleRepository.publishScript("s1", "if (", "script", "groovy");
		RuleDbSyncManager.pollOnce();

		LiteflowResponse response = executor.execute2Resp("chain1", "arg");
		Assertions.assertTrue(response.isSuccess());
		Assertions.assertEquals("old", value(response));
		RuleTargetState state = RuleDbRuntime.scriptState("s1");
		Assertions.assertEquals(2L, state.getDesiredVersion());
		Assertions.assertEquals(1L, state.getActiveVersion());
		Assertions.assertEquals(RuleTargetStatus.FAILED, state.getStatus());
		assertNoCandidateScriptArtifact();
	}

	@Test
	public void testFirstInvalidVersionFailsWithoutActiveGeneration() {
		InMemoryRuleRepository.putScript("s1", "if (", "script", "groovy");
		InMemoryRuleRepository.putChain("chain1", "THEN(a, s1)");
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());

		LiteflowResponse response = executor.execute2Resp("chain1", "arg");

		Assertions.assertFalse(response.isSuccess());
		RuleTargetState state = RuleDbRuntime.scriptState("s1");
		Assertions.assertEquals(0L, state.getActiveVersion());
		Assertions.assertEquals(RuleTargetStatus.FAILED, state.getStatus());
		Assertions.assertNotNull(state.getLastError());
		assertNoCandidateScriptArtifact();
	}

	@Test
	public void testSuccessfulVersionTwoAtomicallyReplacesVersionOne() {
		FlowExecutor executor = loadVersionOne();

		publishVersionTwo();
		LiteflowResponse response = executor.execute2Resp("chain1", "arg");

		Assertions.assertTrue(response.isSuccess());
		Assertions.assertEquals("new", value(response));
		RuleTargetState state = RuleDbRuntime.scriptState("s1");
		Assertions.assertEquals(2L, state.getDesiredVersion());
		Assertions.assertEquals(2L, state.getActiveVersion());
		Assertions.assertEquals(RuleTargetStatus.READY, state.getStatus());
		assertNoCandidateScriptArtifact();
	}

	@Test
	public void testConcurrentRefreshFetchesOnceWhileFollowersUseVersionOne() throws Exception {
		FlowExecutor executor = loadVersionOne();
		publishVersionTwo();
		CountDownLatch candidateFetched = new CountDownLatch(1);
		CountDownLatch releaseCandidate = new CountDownLatch(1);
		InMemoryRuleRepository.afterNextScriptFetch(() -> {
			candidateFetched.countDown();
			await(releaseCandidate);
		});

		ExecutorService pool = Executors.newFixedThreadPool(8);
		try {
			Future<LiteflowResponse> leader = pool.submit(() -> executor.execute2Resp("chain1", "arg"));
			Assertions.assertTrue(candidateFetched.await(5, TimeUnit.SECONDS));
			List<Future<LiteflowResponse>> followers = new ArrayList<>();
			for (int i = 0; i < 6; i++) {
				followers.add(pool.submit(() -> executor.execute2Resp("chain1", "arg")));
			}
			for (Future<LiteflowResponse> follower : followers) {
				LiteflowResponse response = follower.get(2, TimeUnit.SECONDS);
				Assertions.assertTrue(response.isSuccess());
				Assertions.assertEquals("old", value(response));
			}
			Assertions.assertEquals(2, InMemoryRuleRepository.FETCH_SCRIPT_COUNT.get());
			releaseCandidate.countDown();
			LiteflowResponse leaderResponse = leader.get(5, TimeUnit.SECONDS);
			Assertions.assertTrue(leaderResponse.isSuccess());
			Assertions.assertEquals("new", value(leaderResponse));
			Assertions.assertEquals(2L, RuleDbRuntime.scriptState("s1").getActiveVersion());
		} finally {
			releaseCandidate.countDown();
			pool.shutdownNow();
		}
	}

	@Test
	public void testOldArtifactStaysLoadedUntilInFlightExecutionFinishes() throws Exception {
		String blockingOldScript = RuleDbLastGoodScriptTest.class.getName()
				+ ".blockOldScript(); defaultContext.setData(\"val\", \"old\");"
				+ " defaultContext.setData(\"language\", _meta.cmp.refNode.language);";
		InMemoryRuleRepository.putScript("s1", blockingOldScript, "script", "groovy");
		InMemoryRuleRepository.putChain("chain1", "THEN(a, s1)");
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());
		Assertions.assertEquals("old", value(executor.execute2Resp("chain1", "arg")));
		String oldArtifactId = FlowBus.getNode("s1").getRuleDbScriptArtifactId();

		oldScriptStarted = new CountDownLatch(1);
		releaseOldScript = new CountDownLatch(1);
		ExecutorService pool = Executors.newSingleThreadExecutor();
		try {
			Future<LiteflowResponse> oldExecution = pool.submit(() -> executor.execute2Resp("chain1", "arg"));
			Assertions.assertTrue(oldScriptStarted.await(5, TimeUnit.SECONDS));

			InMemoryRuleRepository.publishScript("s1",
					"defaultContext.setData(\"val\", \"new\");", "script", null);
			RuleDbSyncManager.pollOnce();
			LiteflowResponse newResponse = executor.execute2Resp("chain1", "arg");
			Assertions.assertEquals("new", value(newResponse));
			String newArtifactId = FlowBus.getNode("s1").getRuleDbScriptArtifactId();
			List<String> whileInFlight = ScriptExecutorFactory.loadInstance()
					.getScriptExecutor("groovy").getNodeIds();
			Assertions.assertTrue(whileInFlight.contains(oldArtifactId));
			Assertions.assertTrue(ScriptExecutorFactory.loadInstance().getScriptExecutor(null)
					.getNodeIds().contains(newArtifactId));

			releaseOldScript.countDown();
			LiteflowResponse oldResponse = oldExecution.get(5, TimeUnit.SECONDS);
			Assertions.assertEquals("old", value(oldResponse));
			Assertions.assertEquals("groovy",
					oldResponse.getContextBean(DefaultContext.class).getData("language"));
			waitUntil(() -> !ScriptExecutorFactory.loadInstance().getScriptExecutor("groovy")
					.getNodeIds().contains(oldArtifactId), 2000);
		} finally {
			if (releaseOldScript != null) {
				releaseOldScript.countDown();
			}
			oldScriptStarted = null;
			releaseOldScript = null;
			pool.shutdownNow();
		}
	}

	@Test
	public void testInFlightConditionReadsResultFromExecutedScriptType() throws Exception {
		String blockingBooleanScript = RuleDbLastGoodScriptTest.class.getName()
				+ ".blockOldScript(); return true;";
		InMemoryRuleRepository.putScript("s1", blockingBooleanScript, "boolean_script", "groovy");
		InMemoryRuleRepository.putChain("chain1", "IF(s1, a, b)");
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());
		Assertions.assertEquals("s1==>a", executor.execute2Resp("chain1", "arg").getExecuteStepStr());
		Node chainScript = LiteflowMetaOperator.getNodes("chain1", "s1").get(0);

		oldScriptStarted = new CountDownLatch(1);
		releaseOldScript = new CountDownLatch(1);
		ExecutorService pool = Executors.newSingleThreadExecutor();
		try {
			Future<LiteflowResponse> oldExecution = pool.submit(() -> executor.execute2Resp("chain1", "arg"));
			Assertions.assertTrue(oldScriptStarted.await(5, TimeUnit.SECONDS));

			InMemoryRuleRepository.publishScript("s1",
					"defaultContext.setData(\"val\", \"new\");", "script", "groovy");
			RuleDbSyncManager.pollOnce();
			RuleDbRuntime.refreshScript(chainScript);

			releaseOldScript.countDown();
			LiteflowResponse oldResponse = oldExecution.get(5, TimeUnit.SECONDS);
			Assertions.assertTrue(oldResponse.isSuccess());
			Assertions.assertEquals("s1==>a", oldResponse.getExecuteStepStr());
		} finally {
			if (releaseOldScript != null) {
				releaseOldScript.countDown();
			}
			oldScriptStarted = null;
			releaseOldScript = null;
			pool.shutdownNow();
		}
	}

	@Test
	public void testSuccessfulRefreshMarksEveryReferencingChainStale() {
		InMemoryRuleRepository.putScript("s1", oldScript(), "script", "groovy");
		InMemoryRuleRepository.putChain("chainA", "THEN(a, s1)");
		InMemoryRuleRepository.putChain("chainB", "THEN(b, s1)");
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());
		Assertions.assertEquals("old", value(executor.execute2Resp("chainA", "arg")));
		Assertions.assertEquals("old", value(executor.execute2Resp("chainB", "arg")));
		Assertions.assertEquals(RuleTargetStatus.READY, RuleDbRuntime.chainState("chainA").getStatus());
		Assertions.assertEquals(RuleTargetStatus.READY, RuleDbRuntime.chainState("chainB").getStatus());

		publishVersionTwo();
		Assertions.assertEquals("new", value(executor.execute2Resp("chainA", "arg")));

		Assertions.assertEquals(RuleTargetStatus.STALE, RuleDbRuntime.chainState("chainA").getStatus());
		Assertions.assertEquals(RuleTargetStatus.STALE, RuleDbRuntime.chainState("chainB").getStatus());
		Assertions.assertEquals("new", value(executor.execute2Resp("chainB", "arg")));
		Assertions.assertEquals(2L, RuleDbRuntime.scriptState("s1").getActiveVersion());
	}

	@Test
	public void testTypeNameAndLanguageChangeBecomeActiveTogether() {
		InMemoryRuleRepository.putScript("s1", oldScript(), "script", null);
		InMemoryRuleRepository.SCRIPTS.get("s1").setName("old name");
		InMemoryRuleRepository.putChain("chain1", "THEN(a, s1)");
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());
		Assertions.assertTrue(executor.execute2Resp("chain1", "arg").isSuccess());

		InMemoryRuleRepository.publishScript("s1", "return true;", "boolean_script", "groovy");
		InMemoryRuleRepository.SCRIPTS.get("s1").setName("new name");
		RuleDbSyncManager.pollOnce();
		LiteflowResponse response = executor.execute2Resp("chain1", "arg");

		Assertions.assertTrue(response.isSuccess());
		Assertions.assertEquals(NodeTypeEnum.BOOLEAN_SCRIPT, FlowBus.getNode("s1").getType());
		Assertions.assertEquals("new name", FlowBus.getNode("s1").getName());
		Assertions.assertEquals("groovy", FlowBus.getNode("s1").getLanguage());
		Assertions.assertEquals(2L, FlowBus.getNode("s1").getRuleDbScriptVersion());
		assertNoCandidateScriptArtifact();
	}

	@Test
	public void testVersionedArtifactKeyIsNotExposedInScriptMetadata() {
		InMemoryRuleRepository.putScript("s1",
				"defaultContext.setData(\"nodeId\", _meta.nodeId); "
						+ "defaultContext.setData(\"internalVisible\", _meta.containsKey(\"scriptNodeId\"));",
				"script", "groovy");
		InMemoryRuleRepository.putChain("chain1", "THEN(s1)");
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());

		LiteflowResponse response = executor.execute2Resp("chain1", "arg");

		Assertions.assertTrue(response.isSuccess());
		Assertions.assertEquals("s1", response.getContextBean(DefaultContext.class).getData("nodeId"));
		Assertions.assertEquals(Boolean.FALSE,
				response.getContextBean(DefaultContext.class).getData("internalVisible"));
	}

	@Test
	public void testMetadataChangeDuringCandidateBuildKeepsVersionOne() {
		FlowExecutor executor = loadVersionOne();
		publishVersionTwo();
		InMemoryRuleRepository.afterNextScriptFetch(() ->
				InMemoryRuleRepository.putScript("s1", "defaultContext.setData(\"val\", \"newest\");",
						"script", "groovy"));

		LiteflowResponse response = executor.execute2Resp("chain1", "arg");

		Assertions.assertTrue(response.isSuccess());
		Assertions.assertEquals("old", value(response));
		Assertions.assertEquals(1L, RuleDbRuntime.scriptState("s1").getActiveVersion());
		Assertions.assertEquals(RuleTargetStatus.FAILED, RuleDbRuntime.scriptState("s1").getStatus());
		assertNoCandidateScriptArtifact();
	}

	@Test
	public void testDesiredAdvanceDuringCandidateBuildLoadsNewestGenerationNext() {
		FlowExecutor executor = loadVersionOne();
		publishVersionTwo();
		InMemoryRuleRepository.afterNextScriptFetch(() -> {
			InMemoryRuleRepository.publishScript("s1",
					"defaultContext.setData(\"val\", \"newest\");", "script", "groovy");
			RuleDbRuntime.applyChange(lastChange());
		});

		LiteflowResponse oldResponse = executor.execute2Resp("chain1", "arg");
		Assertions.assertTrue(oldResponse.isSuccess());
		Assertions.assertEquals("old", value(oldResponse));
		Assertions.assertEquals(RuleTargetStatus.STALE, RuleDbRuntime.scriptState("s1").getStatus());

		LiteflowResponse newestResponse = executor.execute2Resp("chain1", "arg");
		Assertions.assertTrue(newestResponse.isSuccess());
		Assertions.assertEquals("newest", value(newestResponse));
		Assertions.assertEquals(3L, RuleDbRuntime.scriptState("s1").getActiveVersion());
		assertNoCandidateScriptArtifact();
	}

	@Test
	public void testDeleteAndRecreateDuringCandidateBuildKeepsNewShadowIsolated() {
		FlowExecutor executor = loadVersionOne();
		com.yomahub.liteflow.flow.element.Node oldNode = FlowBus.getNode("s1");
		publishVersionTwo();
		InMemoryRuleRepository.afterNextScriptFetch(() -> {
			InMemoryRuleRepository.deleteScript("s1");
			RuleDbRuntime.applyChange(lastChange());
			InMemoryRuleRepository.publishScript("s1",
					"defaultContext.setData(\"val\", \"recreated\");", "script", "groovy");
			RuleDbRuntime.applyChange(lastChange());
		});

		LiteflowResponse staleResponse = executor.execute2Resp("chain1", "arg");

		Assertions.assertFalse(staleResponse.isSuccess());
		Assertions.assertNotSame(oldNode, FlowBus.getNode("s1"));
		RuleTargetState recreated = RuleDbRuntime.scriptState("s1");
		Assertions.assertEquals(0L, recreated.getActiveVersion());
		Assertions.assertEquals(RuleTargetStatus.SHADOW, recreated.getStatus());

		LiteflowResponse recreatedResponse = executor.execute2Resp("chain1", "arg");
		Assertions.assertTrue(recreatedResponse.isSuccess());
		Assertions.assertEquals("recreated", value(recreatedResponse));
		Assertions.assertEquals(1L, recreated.getActiveVersion());
		assertNoCandidateScriptArtifact();
	}

	private FlowExecutor loadVersionOne() {
		InMemoryRuleRepository.putScript("s1", oldScript(), "script", "groovy");
		InMemoryRuleRepository.putChain("chain1", "THEN(a, s1)");
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());
		LiteflowResponse response = executor.execute2Resp("chain1", "arg");
		Assertions.assertTrue(response.isSuccess());
		Assertions.assertEquals("old", value(response));
		Assertions.assertEquals(1L, RuleDbRuntime.scriptState("s1").getActiveVersion());
		Assertions.assertEquals(RuleTargetStatus.READY, RuleDbRuntime.chainState("chain1").getStatus());
		return executor;
	}

	private static void publishVersionTwo() {
		InMemoryRuleRepository.publishScript("s1",
				"defaultContext.setData(\"val\", \"new\");", "script", "groovy");
		RuleDbSyncManager.pollOnce();
	}

	private static String oldScript() {
		return "defaultContext.setData(\"val\", \"old\");";
	}

	private static Object value(LiteflowResponse response) {
		return response.getContextBean(DefaultContext.class).getData("val");
	}

	private static void assertNoCandidateScriptArtifact() {
		Assertions.assertTrue(ScriptExecutorFactory.loadInstance().getScriptExecutor("groovy").getNodeIds().stream()
				.noneMatch(nodeId -> nodeId.contains("@ruleDbCandidate@")));
		Assertions.assertTrue(ScriptExecutorFactory.loadInstance().getScriptExecutor(null).getNodeIds().stream()
				.noneMatch(nodeId -> nodeId.contains("@ruleDbCandidate@")));
		Assertions.assertTrue(FlowBus.getNodeMap().keySet().stream()
				.noneMatch(nodeId -> nodeId.contains("@ruleDbCandidate@")));
	}

	private static com.yomahub.liteflow.repository.vo.ChangeRecord lastChange() {
		return InMemoryRuleRepository.CHANGES.get(InMemoryRuleRepository.CHANGES.size() - 1);
	}

	public static void blockOldScript() {
		CountDownLatch started = oldScriptStarted;
		CountDownLatch release = releaseOldScript;
		if (started != null && release != null) {
			started.countDown();
			await(release);
		}
	}

	private static void await(CountDownLatch latch) {
		try {
			latch.await(5, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new AssertionError(e);
		}
	}
}
