package com.yomahub.liteflow.repository.sql;

import com.yomahub.liteflow.repository.ChangeSourceHealth;
import com.yomahub.liteflow.repository.RuleChangeListener;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class SqlPollingResilienceTest {

	@Test
	public void repositoryFailureDegradesAndNextSuccessRecovers() {
		AtomicInteger latestCalls = new AtomicInteger();
		StubRepository repository = new StubRepository() {
			@Override
			public long fetchLatestSeq() {
				if (latestCalls.getAndIncrement() == 0) {
					throw new RuntimeException("database unavailable");
				}
				return 0;
			}
		};
		SqlPollingChangeSource source = source(repository, changes -> { });
		try {
			Assertions.assertThrows(RuntimeException.class, source::pollOnce);
			Assertions.assertEquals(ChangeSourceHealth.Status.DEGRADED, source.health().getStatus());
			Assertions.assertEquals(0, source.health().getCursor());

			source.pollOnce();
			Assertions.assertEquals(ChangeSourceHealth.Status.UP, source.health().getStatus());
			Assertions.assertNull(source.health().getRecentError());
		}
		finally {
			source.close();
		}
	}

	@Test
	public void listenerFailureDoesNotAdvanceCursorAndBatchIsRetried() {
		ChangeRecord change = new ChangeRecord(1, ChangeRecord.TargetType.CHAIN,
				"retryChain", ChangeRecord.Op.UPSERT, 1);
		StubRepository repository = new StubRepository() {
			@Override
			public long fetchLatestSeq() {
				return 1;
			}

			@Override
			public List<ChangeRecord> fetchChangesSince(long seq, int limit) {
				return seq < 1 ? Collections.singletonList(change) : Collections.emptyList();
			}
		};
		AtomicInteger deliveries = new AtomicInteger();
		SqlPollingChangeSource source = source(repository, changes -> {
			if (deliveries.getAndIncrement() == 0) {
				throw new RuntimeException("listener failed");
			}
		});
		try {
			Assertions.assertThrows(RuntimeException.class, source::pollOnce);
			Assertions.assertEquals(0, source.health().getCursor());
			Assertions.assertEquals(ChangeSourceHealth.Status.DEGRADED, source.health().getStatus());

			source.pollOnce();
			Assertions.assertEquals(2, deliveries.get());
			Assertions.assertEquals(1, source.health().getCursor());
			Assertions.assertEquals(ChangeSourceHealth.Status.UP, source.health().getStatus());
		}
		finally {
			source.close();
		}
	}

	@Test
	public void advancedWatermarkWithoutReadableRowsRequestsReconcile() {
		StubRepository repository = new StubRepository() {
			@Override
			public long fetchLatestSeq() {
				return 7;
			}
		};
		AtomicInteger reconciles = new AtomicInteger();
		SqlPollingChangeSource source = source(repository, new RuleChangeListener() {
			@Override
			public void onChanges(List<ChangeRecord> changes) {
			}

			@Override
			public void onReconcileRequired() {
				reconciles.incrementAndGet();
			}
		});
		try {
			source.pollOnce();
			Assertions.assertEquals(1, reconciles.get());
			Assertions.assertEquals(0, source.health().getCursor());
			Assertions.assertEquals(ChangeSourceHealth.Status.DEGRADED, source.health().getStatus());
		}
		finally {
			source.close();
		}
	}

	@Test
	public void hundredThousandChangeBacklogIsDrainedInBoundedBatches() {
		final int backlog = 100_000;
		final int batchSize = 128;
		AtomicInteger fetches = new AtomicInteger();
		AtomicInteger largestRepositoryBatch = new AtomicInteger();
		StubRepository repository = new StubRepository() {
			@Override
			public long fetchLatestSeq() {
				return backlog;
			}

			@Override
			public List<ChangeRecord> fetchChangesSince(long seq, int limit) {
				int count = (int) Math.min(limit, backlog - seq);
				List<ChangeRecord> changes = new ArrayList<>(count);
				for (int i = 1; i <= count; i++) {
					changes.add(new ChangeRecord(seq + i, ChangeRecord.TargetType.CHAIN,
							"backlog-" + (seq + i), ChangeRecord.Op.UPSERT, 1));
				}
				fetches.incrementAndGet();
				largestRepositoryBatch.accumulateAndGet(changes.size(), Math::max);
				return changes;
			}
		};
		AtomicInteger delivered = new AtomicInteger();
		AtomicInteger largestDelivery = new AtomicInteger();
		SqlPollingChangeSource source = new SqlPollingChangeSource(repository, 3600, batchSize);
		source.open(changes -> {
			delivered.addAndGet(changes.size());
			largestDelivery.accumulateAndGet(changes.size(), Math::max);
		});
		source.activate(0);
		try {
			source.pollOnce();
			Assertions.assertEquals(backlog, delivered.get());
			Assertions.assertEquals(batchSize, largestRepositoryBatch.get());
			Assertions.assertEquals(batchSize, largestDelivery.get());
			Assertions.assertEquals((backlog + batchSize - 1) / batchSize, fetches.get());
			Assertions.assertEquals(backlog, source.health().getCursor());
		}
		finally {
			source.close();
		}
	}

	private SqlPollingChangeSource source(SqlRuleRepository repository, RuleChangeListener listener) {
		SqlPollingChangeSource source = new SqlPollingChangeSource(repository, 3600, 2);
		source.open(listener);
		source.activate(0);
		return source;
	}

	private static class StubRepository extends SqlRuleRepository {

		StubRepository() {
			super(null, new SqlDialect("lf_"), "test", false);
		}

		@Override
		public long fetchLatestSeq() {
			return 0;
		}

		@Override
		public List<ChangeRecord> fetchChangesSince(long seq, int limit) {
			return Collections.emptyList();
		}
	}
}
