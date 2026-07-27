package com.yomahub.liteflow.repository.etcd;

import com.yomahub.liteflow.repository.ChangeSourceHealth;
import com.yomahub.liteflow.repository.RuleChangeListener;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Resilience contract of the etcd watch change source: reconnect from
 * cursor+1 after a broken stream, reconcile + rebuild after compaction,
 * degrade to reconciliation when delivery fails, and tolerate repeated close.
 */
class EtcdWatchResilienceTest {

	private FakeEtcdWatch watch;
	private EtcdKeys keys;
	private EtcdWatchChangeSource source;
	private List<ChangeRecord> received;
	private AtomicInteger reconciles;

	@BeforeEach
	void setUp() {
		watch = new FakeEtcdWatch();
		keys = new EtcdKeys("/lf", "app");
		source = new EtcdWatchChangeSource(watch, keys, new EtcdRecordCodec());
		received = new CopyOnWriteArrayList<>();
		reconciles = new AtomicInteger();
	}

	@AfterEach
	void tearDown() {
		source.close();
	}

	@Test
	void reconnectsFromCursorPlusOneAfterStreamFailure() {
		source.open(listener());
		source.activate(0);
		emitChainPut("c1", 3);
		Await.until(() -> received.size() == 1);
		assertEquals(3, source.health().getCursor());

		watch.failAll(new RuntimeException("grpc stream broken"));
		Await.until(() -> source.health().getStatus() == ChangeSourceHealth.Status.DEGRADED);
		// first reconnect is scheduled after a 100ms backoff
		Await.until(() -> watch.lastStartRevision(keys.rootPrefix()) == 4);
		assertEquals(1, watch.activeCount());

		emitChainPut("c1", 5);
		Await.until(() -> received.size() == 2);
		assertEquals(5, source.health().getCursor());
		assertEquals(ChangeSourceHealth.Status.UP, source.health().getStatus());
		assertEquals(0, reconciles.get());
	}

	@Test
	void compactionTriggersReconcileThenRebuildsFromNewBaseline() {
		source.open(listener());
		source.activate(0);
		emitChainPut("c1", 3);
		Await.until(() -> received.size() == 1);

		watch.failAll(new EtcdCompactionException(new RuntimeException("etcdserver: mvcc: required revision has been compacted")));
		Await.until(() -> reconciles.get() == 1);
		assertEquals(ChangeSourceHealth.Status.DEGRADED, source.health().getStatus());
		// no blind 5s-rebuild loop for compaction: watchers stay down until reconcile
		assertEquals(0, watch.activeCount());

		source.onReconciled(10);
		assertEquals(11, watch.lastStartRevision(keys.rootPrefix()));
		assertEquals(ChangeSourceHealth.Status.UP, source.health().getStatus());
		assertEquals(10, source.health().getCursor());

		// events below the new baseline are dropped, later ones flow through
		watch.emitPut(keys.chainMeta("c2"), meta(9), 9);
		emitChainPut("c2", 12);
		Await.until(() -> received.size() == 2);
		assertEquals(12, received.get(1).getSeq());
	}

	@Test
	void completedStreamIsTreatedAsRecoverableError() {
		source.open(listener());
		source.activate(0);
		watch.completeAll();
		Await.until(() -> watch.lastStartRevision(keys.rootPrefix()) == 1);
		assertEquals(ChangeSourceHealth.Status.DEGRADED, source.health().getStatus());
		assertEquals(0, reconciles.get());
	}

	@Test
	void deliveryFailureDegradesToReconciliation() {
		AtomicInteger calls = new AtomicInteger();
		source.open(new RuleChangeListener() {
			@Override
			public void onChanges(List<ChangeRecord> changes) {
				if (calls.incrementAndGet() == 1) {
					throw new RuntimeException("consumer blew up");
				}
				received.addAll(changes);
			}

			@Override
			public void onReconcileRequired() {
				reconciles.incrementAndGet();
			}
		});
		source.activate(0);
		emitChainPut("c1", 3);
		Await.until(() -> reconciles.get() == 1);
		assertEquals(ChangeSourceHealth.Status.DEGRADED, source.health().getStatus());
		assertTrue(source.health().getRecentError().contains("consumer blew up"));

		source.onReconciled(3);
		emitChainPut("c1", 4);
		Await.until(() -> received.size() == 1);
		assertEquals(ChangeSourceHealth.Status.UP, source.health().getStatus());
	}

	@Test
	void closeIsIdempotentAndDropsLateCallbacks() {
		source.open(listener());
		source.activate(0);
		source.close();
		source.close();
		assertEquals(ChangeSourceHealth.Status.DOWN, source.health().getStatus());
		assertEquals(0, watch.activeCount());

		// late grpc callbacks must be swallowed, not delivered or rescheduled
		watch.emitPut(keys.chainMeta("c1"), meta(5), 5);
		watch.failAll(new RuntimeException("late error"));
		watch.completeAll();
		assertTrue(received.isEmpty());
		assertEquals(0, reconciles.get());
	}

	@Test
	void validatesConstructorAndListenerArguments() {
		assertThrows(RuntimeException.class, () -> new EtcdWatchChangeSource(null, keys, new EtcdRecordCodec()));
		assertThrows(IllegalArgumentException.class, () -> source.open(null));
		assertFalse(source.requiresContinuousSequence());
	}

	@Test
	void oneRootWatchClassifiesMetadataAndIgnoresContentEvents() {
		source.open(listener());
		source.activate(0);
		assertEquals(1, watch.activeCount());
		assertEquals(0, watch.lastStartRevision(keys.rootPrefix()));

		watch.emitPut(keys.chainContent("c1"), "THEN(a)", 2);
		watch.emitPut(keys.scriptContent("s1"), "return true", 3);
		watch.emitPut(keys.scriptMeta("s1"), meta(4), 4);
		Await.until(() -> received.size() == 1);
		assertEquals(ChangeRecord.TargetType.SCRIPT, received.get(0).getTargetType());
		assertEquals("s1", received.get(0).getTargetId());
		assertEquals(4, source.health().getCursor());
	}

	private RuleChangeListener listener() {
		return new RuleChangeListener() {
			@Override
			public void onChanges(List<ChangeRecord> changes) {
				received.addAll(changes);
			}

			@Override
			public void onReconcileRequired() {
				reconciles.incrementAndGet();
			}
		};
	}

	private void emitChainPut(String chainId, long revision) {
		watch.emitPut(keys.chainMeta(chainId), meta(revision), revision);
	}

	private String meta(long revision) {
		return "{\"version\":" + revision + ",\"md5\":\"m\",\"enable\":true}";
	}
}
