package com.yomahub.liteflow.repository.etcd;

import com.yomahub.liteflow.repository.ChangeSourceHealth;
import com.yomahub.liteflow.repository.RuleChangeListener;
import com.yomahub.liteflow.repository.vo.ChainRecord;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EtcdWatchChangeSourceTest {

	private FakeEtcdWatchFacade watch;
	private EtcdKeys keys;
	private EtcdRecordCodec codec;
	private EtcdWatchChangeSource source;

	@BeforeEach
	void setUp() {
		watch = new FakeEtcdWatchFacade();
		keys = new EtcdKeys("/lf", "app");
		codec = new EtcdRecordCodec();
		source = new EtcdWatchChangeSource(watch, keys, codec);
	}

	@AfterEach
	void tearDown() {
		source.close();
	}

	@Test
	void buffersBeforeActivateAndDropsSnapshotEvents() {
		List<ChangeRecord> changes = new ArrayList<>();
		source.open(changes::addAll);
		watch.emitPut(keys.chainMeta("c1"), chainMeta(1), 10);
		watch.emitPut(keys.chainMeta("c1"), chainMeta(2), 12);

		source.activate(10);
		await(() -> changes.size() == 1);

		assertEquals(12, changes.get(0).getSeq());
		assertEquals(2, changes.get(0).getVersion());
		assertFalse(source.requiresContinuousSequence());
	}

	@Test
	void convertsMetadataDeleteToRuleDelete() {
		List<ChangeRecord> changes = new ArrayList<>();
		source.open(changes::addAll);
		source.activate(5);

		watch.emitDelete(keys.chainMeta("c1"), 8);
		await(() -> changes.size() == 1);
		assertEquals(ChangeRecord.Op.DELETE, changes.get(0).getOp());
	}

	@Test
	void compactedRevisionRequestsReconcileAndRestartsFromNewBaseline() {
		AtomicInteger reconciles = new AtomicInteger();
		source.open(new RuleChangeListener() {
			@Override
			public void onChanges(List<ChangeRecord> changes) { }
			@Override
			public void onReconcileRequired() { reconciles.incrementAndGet(); }
		});
		source.activate(20);

		watch.failActive(new RuntimeException("required revision has been compacted"));
		await(() -> reconciles.get() == 1);
		assertEquals(ChangeSourceHealth.Status.DEGRADED, source.health().getStatus());

		source.onReconciled(30);
		assertTrue(watch.startRevisions().contains(31L));
	}

	private String chainMeta(long version) {
		ChainRecord record = new ChainRecord();
		record.setVersion(version);
		record.setMd5("m" + version);
		record.setEnable(true);
		return codec.encodeChainMeta(record);
	}

	private void await(Check check) {
		long deadline = System.currentTimeMillis() + 2000;
		while (System.currentTimeMillis() < deadline) {
			if (check.done()) {
				return;
			}
			try {
				Thread.sleep(10);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				break;
			}
		}
		throw new AssertionError("condition not met");
	}

	private interface Check {
		boolean done();
	}
}
