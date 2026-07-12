package com.yomahub.liteflow.repository;

import com.yomahub.liteflow.exception.SeqGapException;
import com.yomahub.liteflow.repository.vo.ChainRecord;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import com.yomahub.liteflow.repository.vo.RuleManifest;
import com.yomahub.liteflow.repository.vo.ScriptRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LegacyRuleChangeSourceTest {

	@AfterEach
	void resetAppliedCursor() {
		RuleDbRuntime.LAST_APPLIED_SEQ.set(0);
	}

	@Test
	void reconcileHandoffPreventsRepeatedSequenceGap() {
		GapRepository repository = new GapRepository();
		LegacyRuleChangeSource source = new LegacyRuleChangeSource(repository);
		AtomicInteger reconciles = new AtomicInteger();
		source.open(new RuleChangeListener() {
			@Override
			public void onChanges(List<ChangeRecord> changes) {
			}

			@Override
			public void onReconcileRequired() {
				reconciles.incrementAndGet();
				source.onReconciled(5);
			}
		});
		source.activate(0);

		source.pollOnce();
		source.pollOnce();

		assertEquals(1, reconciles.get());
		assertEquals(1, repository.fetchChangesCalls);
		assertEquals(5, source.health().getCursor());
		source.close();
	}

	@Test
	void subscribeFailureDegradesThenPollingRecovers() {
		RecoveringRepository repository = new RecoveringRepository();
		LegacyRuleChangeSource source = new LegacyRuleChangeSource(repository);
		AtomicInteger delivered = new AtomicInteger();
		source.open(changes -> {
			delivered.addAndGet(changes.size());
			RuleDbRuntime.LAST_APPLIED_SEQ.set(1);
		});
		assertEquals(ChangeSourceHealth.Status.DEGRADED, source.health().getStatus());

		source.activate(0);
		source.pollOnce();

		assertEquals(1, delivered.get());
		assertEquals(ChangeSourceHealth.Status.UP, source.health().getStatus());
		assertEquals(1, source.health().getCursor());
		source.close();
		assertEquals(ChangeSourceHealth.Status.DOWN, source.health().getStatus());
	}

	@Test
	void failedReconcileKeepsUnappliedBatchForRetry() {
		GapBatchRepository repository = new GapBatchRepository();
		LegacyRuleChangeSource source = new LegacyRuleChangeSource(repository);
		AtomicInteger delivered = new AtomicInteger();
		source.open(new RuleChangeListener() {
			@Override
			public void onChanges(List<ChangeRecord> changes) {
				delivered.incrementAndGet();
			}

			@Override
			public void onReconcileRequired() {
				throw new RuntimeException("reconcile unavailable");
			}
		});
		source.activate(0);

		source.pollOnce();

		assertEquals(1, delivered.get());
		assertEquals(0, source.health().getCursor(), "failed reconcile must not advance source cursor");

		RuleDbRuntime.LAST_APPLIED_SEQ.set(3);
		source.pollOnce();

		assertEquals(2, delivered.get(), "retained batch should be retried after reconciliation succeeds");
		assertEquals(3, source.health().getCursor());
		source.close();
	}

	private static class GapRepository extends EmptyRepository {
		private int fetchChangesCalls;

		@Override
		public long fetchLatestSeq() {
			return 5;
		}

		@Override
		public List<ChangeRecord> fetchChangesSince(long seq) {
			fetchChangesCalls++;
			throw new SeqGapException("gap");
		}
	}

	private static class RecoveringRepository extends EmptyRepository {
		@Override
		public void subscribe(RuleChangeListener listener) {
			throw new RuntimeException("subscribe unavailable");
		}

		@Override
		public long fetchLatestSeq() {
			return 1;
		}

		@Override
		public List<ChangeRecord> fetchChangesSince(long seq) {
			return Collections.singletonList(new ChangeRecord(1, ChangeRecord.TargetType.CHAIN,
					"chain", ChangeRecord.Op.UPSERT, 1));
		}
	}

	private static class GapBatchRepository extends EmptyRepository {
		@Override
		public long fetchLatestSeq() {
			return 3;
		}

		@Override
		public List<ChangeRecord> fetchChangesSince(long seq) {
			return java.util.Arrays.asList(
					new ChangeRecord(1, ChangeRecord.TargetType.CHAIN, "first",
							ChangeRecord.Op.UPSERT, 1),
					new ChangeRecord(3, ChangeRecord.TargetType.CHAIN, "third",
							ChangeRecord.Op.UPSERT, 1));
		}
	}

	private abstract static class EmptyRepository implements RuleRepository {
		@Override
		public RuleManifest fetchManifest() {
			return new RuleManifest();
		}

		@Override
		public ChainRecord fetchChain(String chainId) {
			return null;
		}

		@Override
		public ScriptRecord fetchScript(String nodeId) {
			return null;
		}
	}
}
