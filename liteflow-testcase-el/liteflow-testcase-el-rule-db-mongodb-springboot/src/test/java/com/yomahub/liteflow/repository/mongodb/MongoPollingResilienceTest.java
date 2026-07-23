package com.yomahub.liteflow.repository.mongodb;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.yomahub.liteflow.exception.SeqGapException;
import com.yomahub.liteflow.repository.ChangeSourceHealth;
import com.yomahub.liteflow.repository.RuleChangeListener;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Polling resilience scenarios driven by driver-level stubs. The repository is
 * final, so failure injection happens on the mocked MongoDB driver calls that
 * back {@code fetchLatestSeq} and {@code fetchChangesSince}.
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
class MongoPollingResilienceTest {

	private MongoCollection<Document> changes;
	private MongoCollection<Document> sequences;
	private MongoRuleRepository repository;

	@BeforeEach
	void setUp() {
		MongoClient client = mock(MongoClient.class);
		MongoDatabase database = mock(MongoDatabase.class);
		changes = mock(MongoCollection.class);
		sequences = mock(MongoCollection.class);
		when(database.getCollection("lf_change_log")).thenReturn(changes);
		when(database.getCollection("lf_sequence")).thenReturn(sequences);
		repository = new MongoRuleRepository(client, database, new MongoCollections("lf_"), "app");
	}

	@AfterEach
	void tearDown() {
		repository = null;
	}

	@Test
	void repositoryFailureDegradesAndNextSuccessRecovers() {
		AtomicInteger latestCalls = new AtomicInteger();
		when(sequences.find(any(Bson.class))).thenAnswer(invocation -> {
			if (latestCalls.getAndIncrement() == 0) {
				throw new RuntimeException("database unavailable");
			}
			return latestSeqIterable(null);
		});
		MongoPollingChangeSource source = source(changes -> { });
		try {
			assertThrows(RuntimeException.class, source::pollOnce);
			assertEquals(ChangeSourceHealth.Status.DEGRADED, source.health().getStatus());
			assertEquals(0, source.health().getCursor());

			source.pollOnce();
			assertEquals(ChangeSourceHealth.Status.UP, source.health().getStatus());
			assertNull(source.health().getRecentError());
		}
		finally {
			source.close();
		}
	}

	@Test
	void listenerFailureDoesNotAdvanceCursorAndBatchIsRetried() {
		FindIterable<Document> latestSeq = latestSeqIterable(new Document("seq", 1L));
		when(sequences.find(any(Bson.class))).thenReturn(latestSeq);
		stubChangeWindow(new Document("seq", 1L).append("targetType", "CHAIN")
				.append("targetId", "retryChain").append("operation", "UPSERT").append("version", 1L));
		AtomicInteger deliveries = new AtomicInteger();
		MongoPollingChangeSource source = source(changes -> {
			if (deliveries.getAndIncrement() == 0) {
				throw new RuntimeException("listener failed");
			}
		});
		try {
			assertThrows(RuntimeException.class, source::pollOnce);
			assertEquals(0, source.health().getCursor());
			assertEquals(ChangeSourceHealth.Status.DEGRADED, source.health().getStatus());

			source.pollOnce();
			assertEquals(2, deliveries.get());
			assertEquals(1, source.health().getCursor());
			assertEquals(ChangeSourceHealth.Status.UP, source.health().getStatus());
		}
		finally {
			source.close();
		}
	}

	@Test
	void advancedWatermarkWithoutReadableRowsRequestsReconcile() {
		FindIterable<Document> latestSeq = latestSeqIterable(new Document("seq", 7L));
		when(sequences.find(any(Bson.class))).thenReturn(latestSeq);
		stubChangeWindow();
		AtomicInteger reconciles = new AtomicInteger();
		MongoPollingChangeSource source = source(new RuleChangeListener() {
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
			assertEquals(1, reconciles.get());
			assertEquals(0, source.health().getCursor());
			assertEquals(ChangeSourceHealth.Status.DEGRADED, source.health().getStatus());
		}
		finally {
			source.close();
		}
	}

	@Test
	void sequenceGapRequestsReconcileAndStaysDegraded() {
		FindIterable<Document> latestSeq = latestSeqIterable(new Document("seq", 5L));
		when(sequences.find(any(Bson.class))).thenReturn(latestSeq);
		stubChangeWindow(new Document("seq", 5L).append("targetType", "CHAIN")
				.append("targetId", "gapChain").append("operation", "UPSERT").append("version", 1L));
		AtomicInteger reconciles = new AtomicInteger();
		MongoPollingChangeSource source = source(new RuleChangeListener() {
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
			assertEquals(1, reconciles.get());
			assertEquals(0, source.health().getCursor());
			assertEquals(ChangeSourceHealth.Status.DEGRADED, source.health().getStatus());
			assertTrue(source.health().getRecentError().contains("changelog gap"));
		}
		finally {
			source.close();
		}
	}

	@Test
	void changeLogCorruptionSurfacesAsSeqGapFamilyError() {
		FindIterable<Document> latestSeq = latestSeqIterable(new Document("seq", 1L));
		when(sequences.find(any(Bson.class))).thenReturn(latestSeq);
		stubChangeWindow(new Document("seq", 1L).append("targetType", "BROKEN")
				.append("targetId", "c1").append("operation", "UPSERT").append("version", 1L));
		MongoPollingChangeSource source = source(changes -> { });
		try {
			source.pollOnce();
			assertEquals(ChangeSourceHealth.Status.DEGRADED, source.health().getStatus());
			assertTrue(source.health().getRecentError().contains("is invalid"));
		}
		finally {
			source.close();
		}
	}

	@Test
	void hundredThousandChangeBacklogIsDrainedInBoundedBatches() {
		final int backlog = 100_000;
		final int batchSize = 128;
		FindIterable<Document> latestSeq = latestSeqIterable(new Document("seq", (long) backlog));
		when(sequences.find(any(Bson.class))).thenReturn(latestSeq);
		AtomicLong served = new AtomicLong();
		AtomicInteger fetches = new AtomicInteger();
		AtomicInteger largestRepositoryBatch = new AtomicInteger();
		FindIterable<Document> iterable = mock(FindIterable.class);
		when(changes.find(any(Bson.class))).thenReturn(iterable);
		when(iterable.sort(any(Bson.class))).thenReturn(iterable);
		when(iterable.limit(anyInt())).thenReturn(iterable);
		when(iterable.into(any(List.class))).thenAnswer(invocation -> {
			List<Document> target = invocation.getArgument(0);
			int count = (int) Math.min(batchSize, backlog - served.get());
			for (int i = 1; i <= count; i++) {
				long seq = served.get() + i;
				target.add(new Document("seq", seq).append("targetType", "CHAIN")
						.append("targetId", "backlog-" + seq).append("operation", "UPSERT").append("version", 1L));
			}
			served.addAndGet(count);
			fetches.incrementAndGet();
			largestRepositoryBatch.accumulateAndGet(count, Math::max);
			return target;
		});
		AtomicInteger delivered = new AtomicInteger();
		AtomicInteger largestDelivery = new AtomicInteger();
		MongoPollingChangeSource source = new MongoPollingChangeSource(repository, 3600, batchSize);
		source.open(batch -> {
			delivered.addAndGet(batch.size());
			largestDelivery.accumulateAndGet(batch.size(), Math::max);
		});
		source.activate(0);
		try {
			source.pollOnce();
			assertEquals(backlog, delivered.get());
			assertEquals(batchSize, largestRepositoryBatch.get());
			assertEquals(batchSize, largestDelivery.get());
			assertEquals((backlog + batchSize - 1) / batchSize, fetches.get());
			assertEquals(backlog, source.health().getCursor());
		}
		finally {
			source.close();
		}
	}

	@Test
	void scheduledPollSwallowsFailuresAndReportsDegradedHealth() throws Exception {
		when(sequences.find(any(Bson.class))).thenThrow(new RuntimeException("database unavailable"));
		MongoPollingChangeSource source = new MongoPollingChangeSource(repository, 1, 2);
		source.open(changes -> { });
		source.activate(0);
		try {
			long deadline = System.currentTimeMillis() + 10_000;
			while (source.health().getStatus() != ChangeSourceHealth.Status.DEGRADED
					&& System.currentTimeMillis() < deadline) {
				Thread.sleep(50);
			}
			assertEquals(ChangeSourceHealth.Status.DEGRADED, source.health().getStatus());
		}
		finally {
			source.close();
		}
		assertEquals(ChangeSourceHealth.Status.DOWN, source.health().getStatus());
	}

	private MongoPollingChangeSource source(RuleChangeListener listener) {
		MongoPollingChangeSource source = new MongoPollingChangeSource(repository, 3600, 2);
		source.open(listener);
		source.activate(0);
		return source;
	}

	private FindIterable<Document> latestSeqIterable(Document first) {
		FindIterable<Document> iterable = mock(FindIterable.class);
		when(iterable.projection(any(Bson.class))).thenReturn(iterable);
		when(iterable.first()).thenReturn(first);
		return iterable;
	}

	private void stubChangeWindow(Document... documents) {
		FindIterable<Document> iterable = mock(FindIterable.class);
		when(changes.find(any(Bson.class))).thenReturn(iterable);
		when(iterable.sort(any(Bson.class))).thenReturn(iterable);
		when(iterable.limit(anyInt())).thenReturn(iterable);
		when(iterable.into(any(List.class))).thenAnswer(invocation -> {
			List<Document> target = invocation.getArgument(0);
			Collections.addAll(target, documents);
			return target;
		});
	}
}
