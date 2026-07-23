package com.yomahub.liteflow.repository.zk;

import com.yomahub.liteflow.repository.ChangeSourceHealth;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deterministic unit tests for {@link ZkCacheChangeSource}: events are driven
 * directly through the cache callback so buffering, replay, de-duplication and
 * degradation can be asserted without waiting on real watch timing.
 */
class ZkCacheChangeSourceTest {

	private TestingServer server;
	private CuratorFramework client;
	private ZkPaths paths;
	private ZkRecordCodec codec;
	private ZkTestSupport.RecordingListener listener;
	private ZkCacheChangeSource source;

	@BeforeEach
	void setUp() throws Exception {
		server = ZkTestSupport.server();
		client = ZkTestSupport.client(server, 5000);
		paths = new ZkPaths("/lf", "unit");
		codec = new ZkRecordCodec();
		ZkTestSupport.createRoots(client, paths);
		listener = new ZkTestSupport.RecordingListener();
		source = new ZkCacheChangeSource(client, paths, codec);
	}

	@AfterEach
	void tearDown() throws Exception {
		if (source != null) { source.close(); }
		if (client != null) { client.close(); }
		if (server != null) { server.close(); }
	}

	@Test
	void openRejectsNullListenerAndSecondOpenIsIgnored() {
		assertThrows(IllegalArgumentException.class, () -> source.open(null));
		source.open(listener);
		source.open(changes -> { });
		assertFalse(source.requiresContinuousSequence());
		source.activate(0);
		source.activate(0);
	}

	@Test
	void eventsAreBufferedUntilActivateThenReplayedAboveBaseline() throws Exception {
		source.open(listener);
		chainEvent(CuratorCacheListener.Type.NODE_CREATED, "c1", 5, meta("c1", 1, true));
		chainEvent(CuratorCacheListener.Type.NODE_CREATED, "c2", 9, meta("c2", 1, true));
		Thread.sleep(200);
		assertTrue(listener.changes.isEmpty());

		source.activate(6);
		ZkTestSupport.await("replay above baseline", () -> listener.changes.size() == 1);
		assertEquals("c2", listener.changes.get(0).getTargetId());
		assertEquals(9, source.health().getCursor());
		assertEquals(ChangeSourceHealth.Status.UP, source.health().getStatus());
		ZkTestSupport.await("activate requests reconcile", () -> listener.reconciles.get() >= 1);
	}

	@Test
	void upsertUpdateAndDeleteAreDeliveredInOrder() throws Exception {
		source.open(listener);
		source.activate(0);

		// fabricated revisions must stay below the real root pzxid so the
		// delete revision (derived from the root znode) stays deliverable
		chainEvent(CuratorCacheListener.Type.NODE_CREATED, "c1", 1, meta("c1", 1, true));
		ZkTestSupport.await("create delivered", () -> listener.changes.size() == 1);
		ChangeRecord created = listener.changes.get(0);
		assertEquals(ChangeRecord.Op.UPSERT, created.getOp());
		assertEquals(1, created.getVersion());
		assertEquals(ChangeRecord.TargetType.CHAIN, created.getTargetType());

		chainEvent(CuratorCacheListener.Type.NODE_CHANGED, "c1", 2, meta("c1", 2, true));
		ZkTestSupport.await("update delivered", () -> listener.changes.size() == 2);
		assertEquals(2, listener.changes.get(1).getVersion());

		chainEvent(CuratorCacheListener.Type.NODE_DELETED, "c1", 2, meta("c1", 2, true));
		ZkTestSupport.await("delete delivered", () -> listener.changes.size() == 3);
		ChangeRecord deleted = listener.changes.get(2);
		assertEquals(ChangeRecord.Op.DELETE, deleted.getOp());
		assertTrue(deleted.getSeq() > 0);
	}

	@Test
	void staleOrDuplicateEventsAreDeduplicatedPerTarget() throws Exception {
		source.open(listener);
		source.activate(0);

		chainEvent(CuratorCacheListener.Type.NODE_CREATED, "c1", 10, meta("c1", 1, true));
		ZkTestSupport.await("first delivery", () -> listener.changes.size() == 1);

		chainEvent(CuratorCacheListener.Type.NODE_CHANGED, "c1", 10, meta("c1", 1, true));
		chainEvent(CuratorCacheListener.Type.NODE_CHANGED, "c1", 7, meta("c1", 1, true));
		Thread.sleep(300);
		assertEquals(1, listener.changes.size());
		assertEquals(10, source.health().getCursor());
	}

	@Test
	void disabledMetadataIsDeliveredAsDelete() throws Exception {
		source.open(listener);
		source.activate(0);

		chainEvent(CuratorCacheListener.Type.NODE_CHANGED, "c1", 10, meta("c1", 3, false));
		ZkTestSupport.await("disable delivered as delete", () -> listener.changes.size() == 1);
		assertEquals(ChangeRecord.Op.DELETE, listener.changes.get(0).getOp());
	}

	@Test
	void deleteWithoutResolvableRevisionIsDroppedAndReconciled() throws Exception {
		// use paths whose metadata root does not exist so deletionRevision fails
		ZkPaths orphanPaths = new ZkPaths("/lf", "orphan");
		ZkCacheChangeSource orphanSource = new ZkCacheChangeSource(client, orphanPaths, codec);
		ZkTestSupport.RecordingListener orphanListener = new ZkTestSupport.RecordingListener();
		orphanSource.open(orphanListener);
		orphanSource.activate(0);
		try {
			int reconcilesBefore = orphanListener.reconciles.get();
			orphanSource.onEvent(orphanPaths.chainMetaRoot(), ChangeRecord.TargetType.CHAIN,
					CuratorCacheListener.Type.NODE_DELETED,
					ZkTestSupport.childData(orphanPaths.chainMeta("gone"), 12, meta("gone", 2, true)), null);

			ZkTestSupport.await("unresolvable delete degrades health", () ->
					orphanSource.health().getStatus() == ChangeSourceHealth.Status.DEGRADED);
			ZkTestSupport.await("unresolvable delete requests reconcile",
					() -> orphanListener.reconciles.get() > reconcilesBefore);
			Thread.sleep(200);
			assertTrue(orphanListener.changes.isEmpty());
		}
		finally {
			orphanSource.close();
		}
	}

	@Test
	void listenerFailureMarksDegradedAndNextSuccessRecovers() throws Exception {
		source.open(listener);
		source.activate(0);
		listener.failNextDeliveries(1);

		chainEvent(CuratorCacheListener.Type.NODE_CREATED, "c1", 10, meta("c1", 1, true));
		ZkTestSupport.await("listener failure degrades health",
				() -> source.health().getStatus() == ChangeSourceHealth.Status.DEGRADED);

		chainEvent(CuratorCacheListener.Type.NODE_CREATED, "c2", 11, meta("c2", 1, true));
		ZkTestSupport.await("next delivery recovers", () -> listener.changes.size() == 1);
		assertEquals(ChangeSourceHealth.Status.UP, source.health().getStatus());
	}

	@Test
	void malformedEventMarksDegradedAndRequestsReconcile() throws Exception {
		source.open(listener);
		source.activate(0);
		int reconcilesBefore = listener.reconciles.get();

		source.onEvent(paths.chainMetaRoot(), ChangeRecord.TargetType.CHAIN,
				CuratorCacheListener.Type.NODE_CREATED, null,
				ZkTestSupport.childData(paths.chainMetaRoot() + "/a/b", 10, meta("a", 1, true)));

		ZkTestSupport.await("malformed event degrades health",
				() -> source.health().getStatus() == ChangeSourceHealth.Status.DEGRADED);
		ZkTestSupport.await("malformed event requests reconcile",
				() -> listener.reconciles.get() > reconcilesBefore);
	}

	@Test
	void eventsForRootItselfOrNullDataAreIgnored() throws Exception {
		source.open(listener);
		source.activate(0);

		source.onEvent(paths.chainMetaRoot(), ChangeRecord.TargetType.CHAIN,
				CuratorCacheListener.Type.NODE_CREATED, null,
				ZkTestSupport.childData(paths.chainMetaRoot(), 10, meta("root", 1, true)));
		source.onEvent(paths.chainMetaRoot(), ChangeRecord.TargetType.CHAIN,
				CuratorCacheListener.Type.NODE_CREATED, null, null);
		Thread.sleep(200);
		assertTrue(listener.changes.isEmpty());
		assertEquals(ChangeSourceHealth.Status.UP, source.health().getStatus());
	}

	@Test
	void onReconciledResetsDeduplicationAndAdvancesCursor() throws Exception {
		source.open(listener);
		source.activate(0);
		chainEvent(CuratorCacheListener.Type.NODE_CREATED, "c1", 10, meta("c1", 1, true));
		ZkTestSupport.await("first delivery", () -> listener.changes.size() == 1);

		source.onReconciled(3);
		assertEquals(10, source.health().getCursor());

		chainEvent(CuratorCacheListener.Type.NODE_CHANGED, "c1", 5, meta("c1", 2, true));
		ZkTestSupport.await("redelivery after reconcile", () -> listener.changes.size() == 2);
	}

	@Test
	void eventsAfterCloseAreIgnored() throws Exception {
		source.open(listener);
		source.activate(0);
		source.close();

		chainEvent(CuratorCacheListener.Type.NODE_CREATED, "c1", 10, meta("c1", 1, true));
		Thread.sleep(200);
		assertTrue(listener.changes.isEmpty());
		assertEquals(ChangeSourceHealth.Status.DOWN, source.health().getStatus());
	}

	private void chainEvent(CuratorCacheListener.Type type, String chainId, long mzxid, byte[] metadata) {
		String path = paths.chainMeta(chainId);
		if (type == CuratorCacheListener.Type.NODE_DELETED) {
			source.onEvent(paths.chainMetaRoot(), ChangeRecord.TargetType.CHAIN, type,
					ZkTestSupport.childData(path, mzxid, metadata), null);
		}
		else {
			source.onEvent(paths.chainMetaRoot(), ChangeRecord.TargetType.CHAIN, type, null,
					ZkTestSupport.childData(path, mzxid, metadata));
		}
	}

	private byte[] meta(String chainId, long version, boolean enabled) {
		com.yomahub.liteflow.repository.vo.ChainRecord record =
				ZkTestSupport.chainRecord(chainId, version, "THEN(a)");
		record.setEnable(enabled);
		return codec.encodeChainMeta(record);
	}
}
