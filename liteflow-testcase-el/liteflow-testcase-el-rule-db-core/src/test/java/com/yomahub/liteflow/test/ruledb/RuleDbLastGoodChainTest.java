package com.yomahub.liteflow.test.ruledb;

import com.yomahub.liteflow.core.FlowExecutor;
import com.yomahub.liteflow.flow.FlowBus;
import com.yomahub.liteflow.flow.LiteflowResponse;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.repository.RuleDbRuntime;
import com.yomahub.liteflow.repository.RuleDbSyncManager;
import com.yomahub.liteflow.repository.runtime.RuleTargetState;
import com.yomahub.liteflow.repository.runtime.RuleTargetStatus;
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

public class RuleDbLastGoodChainTest extends BaseRuleDbTest {

	@Test
	public void testInvalidNewElKeepsVersionOneExecutable() {
		FlowExecutor executor = loadVersionOne("THEN(a, b)");

		InMemoryRuleRepository.publishChain("chain1", "THEN(a, missing)");
		RuleDbSyncManager.pollOnce();

		LiteflowResponse response = executor.execute2Resp("chain1", "arg");
		Assertions.assertTrue(response.isSuccess());
		Assertions.assertEquals("a==>b", response.getExecuteStepStr());
		RuleTargetState state = RuleDbRuntime.chainState("chain1");
		Assertions.assertEquals(2L, state.getDesiredVersion());
		Assertions.assertEquals(1L, state.getActiveVersion());
		Assertions.assertEquals(RuleTargetStatus.FAILED, state.getStatus());
	}

	@Test
	public void testFirstInvalidVersionFailsWithoutActiveGeneration() {
		InMemoryRuleRepository.putChain("chain1", "THEN(a, missing)");
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());

		LiteflowResponse response = executor.execute2Resp("chain1", "arg");

		Assertions.assertFalse(response.isSuccess());
		RuleTargetState state = RuleDbRuntime.chainState("chain1");
		Assertions.assertEquals(0L, state.getActiveVersion());
		Assertions.assertEquals(RuleTargetStatus.FAILED, state.getStatus());
		Assertions.assertNotNull(state.getLastError());
	}

	@Test
	public void testSuccessfulVersionTwoAtomicallyReplacesVersionOne() {
		FlowExecutor executor = loadVersionOne("THEN(a, b)");

		InMemoryRuleRepository.publishChain("chain1", "THEN(b, a)");
		RuleDbSyncManager.pollOnce();

		LiteflowResponse response = executor.execute2Resp("chain1", "arg");
		Assertions.assertTrue(response.isSuccess());
		Assertions.assertEquals("b==>a", response.getExecuteStepStr());
		RuleTargetState state = RuleDbRuntime.chainState("chain1");
		Assertions.assertEquals(2L, state.getDesiredVersion());
		Assertions.assertEquals(2L, state.getActiveVersion());
		Assertions.assertEquals(RuleTargetStatus.READY, state.getStatus());
		assertNoCandidateChainPublished();
	}

	@Test
	public void testConcurrentRefreshFetchesOnceWhileFollowersUseVersionOne() throws Exception {
		FlowExecutor executor = loadVersionOne("THEN(a, b)");
		InMemoryRuleRepository.publishChain("chain1", "THEN(b, a)");
		RuleDbSyncManager.pollOnce();
		CountDownLatch candidateFetched = new CountDownLatch(1);
		CountDownLatch releaseCandidate = new CountDownLatch(1);
		InMemoryRuleRepository.afterNextChainFetch(() -> {
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
				Assertions.assertEquals("a==>b", response.getExecuteStepStr());
			}
			Assertions.assertEquals(2, InMemoryRuleRepository.FETCH_CHAIN_COUNT.get());
			releaseCandidate.countDown();
			LiteflowResponse leaderResponse = leader.get(5, TimeUnit.SECONDS);
			Assertions.assertTrue(leaderResponse.isSuccess());
			Assertions.assertEquals("b==>a", leaderResponse.getExecuteStepStr());
			Assertions.assertEquals(2L, RuleDbRuntime.chainState("chain1").getActiveVersion());
		} finally {
			releaseCandidate.countDown();
			pool.shutdownNow();
		}
	}

	@Test
	public void testMetadataChangeBetweenContentReadsRejectsCandidate() {
		FlowExecutor executor = loadVersionOne("THEN(a, b)");
		InMemoryRuleRepository.publishChain("chain1", "THEN(b, a)");
		RuleDbSyncManager.pollOnce();
		InMemoryRuleRepository.afterNextChainFetch(
				() -> InMemoryRuleRepository.putChain("chain1", "THEN(c, a)"));

		LiteflowResponse response = executor.execute2Resp("chain1", "arg");

		Assertions.assertTrue(response.isSuccess());
		Assertions.assertEquals("a==>b", response.getExecuteStepStr());
		RuleTargetState state = RuleDbRuntime.chainState("chain1");
		Assertions.assertEquals(1L, state.getActiveVersion());
		Assertions.assertEquals(RuleTargetStatus.FAILED, state.getStatus());
		assertNoCandidateChainPublished();
	}

	@Test
	public void testDesiredAdvanceDuringCandidateBuildRetriesNewestGeneration() {
		FlowExecutor executor = loadVersionOne("THEN(a, b)");
		InMemoryRuleRepository.publishChain("chain1", "THEN(b, a)");
		RuleDbSyncManager.pollOnce();
		InMemoryRuleRepository.afterNextChainFetch(() -> {
			InMemoryRuleRepository.publishChain("chain1", "THEN(c, a)");
			RuleDbRuntime.applyChange(lastChange());
		});

		LiteflowResponse oldResponse = executor.execute2Resp("chain1", "arg");
		Assertions.assertTrue(oldResponse.isSuccess());
		Assertions.assertEquals("a==>b", oldResponse.getExecuteStepStr());
		Assertions.assertEquals(RuleTargetStatus.STALE, RuleDbRuntime.chainState("chain1").getStatus());

		LiteflowResponse newestResponse = executor.execute2Resp("chain1", "arg");
		Assertions.assertTrue(newestResponse.isSuccess());
		Assertions.assertEquals("c==>a", newestResponse.getExecuteStepStr());
		Assertions.assertEquals(3L, RuleDbRuntime.chainState("chain1").getActiveVersion());
	}

	@Test
	public void testDeleteDuringCandidateBuildCannotActivateFetchedVersion() {
		FlowExecutor executor = loadVersionOne("THEN(a, b)");
		InMemoryRuleRepository.publishChain("chain1", "THEN(b, a)");
		RuleDbSyncManager.pollOnce();
		InMemoryRuleRepository.afterNextChainFetch(() -> {
			InMemoryRuleRepository.deleteChain("chain1");
			RuleDbRuntime.applyChange(lastChange());
		});

		LiteflowResponse response = executor.execute2Resp("chain1", "arg");

		Assertions.assertFalse(response.isSuccess());
		Assertions.assertFalse(FlowBus.containChain("chain1"));
		RuleTargetState state = RuleDbRuntime.chainState("chain1");
		Assertions.assertEquals(1L, state.getActiveVersion());
		Assertions.assertEquals(RuleTargetStatus.DELETED, state.getStatus());
		assertNoCandidateChainPublished();
	}

	@Test
	public void testDeleteAndRecreateDuringCandidateBuildKeepsNewShadowIsolated() {
		FlowExecutor executor = loadVersionOne("THEN(a, b)");
		com.yomahub.liteflow.flow.element.Chain oldChain = FlowBus.getChain("chain1");
		InMemoryRuleRepository.publishChain("chain1", "THEN(b, a)");
		RuleDbSyncManager.pollOnce();
		InMemoryRuleRepository.afterNextChainFetch(() -> {
			InMemoryRuleRepository.deleteChain("chain1");
			RuleDbRuntime.applyChange(lastChange());
			InMemoryRuleRepository.publishChain("chain1", "THEN(c)");
			RuleDbRuntime.applyChange(lastChange());
		});

		LiteflowResponse staleResponse = executor.execute2Resp("chain1", "arg");

		Assertions.assertFalse(staleResponse.isSuccess());
		Assertions.assertNotSame(oldChain, FlowBus.getChain("chain1"));
		RuleTargetState recreated = RuleDbRuntime.chainState("chain1");
		Assertions.assertEquals(0L, recreated.getActiveVersion());
		Assertions.assertEquals(RuleTargetStatus.SHADOW, recreated.getStatus());

		LiteflowResponse recreatedResponse = executor.execute2Resp("chain1", "arg");
		Assertions.assertTrue(recreatedResponse.isSuccess());
		Assertions.assertEquals("c", recreatedResponse.getExecuteStepStr());
		Assertions.assertEquals(1L, recreated.getActiveVersion());
		assertNoCandidateChainPublished();
	}

	@Test
	public void testCandidateSupportsLazySubChainAndScriptReferences() {
		InMemoryRuleRepository.putChain("chain1", "THEN(a)");
		InMemoryRuleRepository.putChain("sub1", "THEN(b)");
		InMemoryRuleRepository.putScript("s1", "defaultContext.setData(\"candidateScript\", true);",
				"script", "groovy");
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());
		Assertions.assertTrue(executor.execute2Resp("chain1", "arg").isSuccess());

		InMemoryRuleRepository.publishChain("chain1", "THEN(sub1, s1, c)");
		RuleDbSyncManager.pollOnce();
		LiteflowResponse response = executor.execute2Resp("chain1", "arg");

		Assertions.assertTrue(response.isSuccess());
		Assertions.assertEquals("b==>s1==>c", response.getExecuteStepStr());
		Assertions.assertEquals(Boolean.TRUE,
				response.getContextBean(DefaultContext.class).getData("candidateScript"));
		Assertions.assertEquals(1L, RuleDbRuntime.chainState("sub1").getActiveVersion());
		assertNoCandidateChainPublished();
	}

	private FlowExecutor loadVersionOne(String el) {
		InMemoryRuleRepository.putChain("chain1", el);
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());
		LiteflowResponse response = executor.execute2Resp("chain1", "arg");
		Assertions.assertTrue(response.isSuccess());
		Assertions.assertEquals(1L, RuleDbRuntime.chainState("chain1").getActiveVersion());
		return executor;
	}

	private static void await(CountDownLatch latch) {
		try {
			latch.await(5, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new AssertionError(e);
		}
	}

	private static com.yomahub.liteflow.repository.vo.ChangeRecord lastChange() {
		return InMemoryRuleRepository.CHANGES.get(InMemoryRuleRepository.CHANGES.size() - 1);
	}

	private static void assertNoCandidateChainPublished() {
		Assertions.assertTrue(FlowBus.getChainMap().keySet().stream()
				.noneMatch(chainId -> chainId.contains("@ruleDb@")));
	}
}
