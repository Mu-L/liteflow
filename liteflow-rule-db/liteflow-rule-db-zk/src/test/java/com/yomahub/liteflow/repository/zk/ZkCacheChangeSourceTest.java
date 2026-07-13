package com.yomahub.liteflow.repository.zk;

import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.PublishResult;
import com.yomahub.liteflow.publisher.RemoveRuleRequest;
import com.yomahub.liteflow.repository.RuleChangeListener;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ZkCacheChangeSourceTest {

	private TestingServer server;
	private CuratorFramework client;
	private ZkPaths paths;
	private ZkRecordCodec codec;
	private ZkRulePublisher publisher;
	private ZkRuleRepository repository;
	private ZkCacheChangeSource source;

	@BeforeEach
	void setUp() throws Exception {
		server = new TestingServer(true);
		client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(100));
		client.start();
		assertTrue(client.blockUntilConnected(5, TimeUnit.SECONDS));
		paths = new ZkPaths("/lf", "app");
		codec = new ZkRecordCodec();
		publisher = new ZkRulePublisher(client, paths, codec);
		repository = new ZkRuleRepository(client, paths, codec);
		source = new ZkCacheChangeSource(client, paths, codec);
	}

	@AfterEach
	void tearDown() throws Exception {
		if (source != null) { source.close(); }
		if (publisher != null) { publisher.close(); }
		if (client != null) { client.close(); }
		if (server != null) { server.close(); }
	}

	@Test
	void buffersEventsUntilManifestActivationAndReconciles() {
		List<ChangeRecord> received = new ArrayList<>();
		AtomicInteger reconciles = new AtomicInteger();
		PublishResult one = publisher.publishChain(chain("c1", "THEN(a)", 0L));
		source.open(listener(received, reconciles));
		long baseline = repository.fetchManifest().getLatestSeq();
		publisher.publishChain(chain("c1", "THEN(b)", one.getVersion()));

		source.activate(baseline);
		await(() -> received.size() == 1 && reconciles.get() >= 1);

		assertEquals(2, received.get(0).getVersion());
		assertFalse(source.requiresContinuousSequence());
	}

	@Test
	void deliversLiveUpdateAndDelete() {
		List<ChangeRecord> received = new ArrayList<>();
		PublishResult one = publisher.publishChain(chain("c1", "THEN(a)", 0L));
		source.open(received::addAll);
		source.activate(repository.fetchManifest().getLatestSeq());

		PublishResult two = publisher.publishChain(chain("c1", "THEN(b)", one.getVersion()));
		await(() -> contains(received, ChangeRecord.Op.UPSERT, two.getVersion()));
		publisher.removeChain(RemoveRuleRequest.builder()
				.targetId("c1").expectedVersion(two.getVersion()).build());
		await(() -> contains(received, ChangeRecord.Op.DELETE, two.getVersion()));
	}

	@Test
	void manualContentAndMetadataVersionUpdateIsObserved() throws Exception {
		List<ChangeRecord> received = new ArrayList<>();
		publisher.publishChain(chain("c1", "THEN(a)", 0L));
		source.open(received::addAll);
		source.activate(repository.fetchManifest().getLatestSeq());

		byte[] metadata = client.getData().forPath(paths.chainMeta("c1"));
		com.yomahub.liteflow.repository.vo.ChainRecord record = codec.decodeChain(
				"c1", metadata, client.getData().forPath(paths.chainContent("c1")));
		record.setVersion(2);
		client.transaction().forOperations(
				client.transactionOp().setData().forPath(paths.chainContent("c1"), codec.encodeContent(2, "THEN(b)")),
				client.transactionOp().setData().forPath(paths.chainMeta("c1"), codec.encodeChainMeta(record)));

		await(() -> contains(received, ChangeRecord.Op.UPSERT, 2));
		assertEquals("THEN(b)", repository.fetchChain("c1").getEl());
	}

	private RuleChangeListener listener(List<ChangeRecord> received, AtomicInteger reconciles) {
		return new RuleChangeListener() {
			@Override public void onChanges(List<ChangeRecord> changes) { received.addAll(changes); }
			@Override public void onReconcileRequired() { reconciles.incrementAndGet(); }
		};
	}

	private boolean contains(List<ChangeRecord> changes, ChangeRecord.Op operation, long version) {
		for (ChangeRecord change : new ArrayList<>(changes)) {
			if (change.getOp() == operation && change.getVersion() == version) { return true; }
		}
		return false;
	}

	private PublishChainRequest chain(String id, String el, Long expected) {
		return PublishChainRequest.builder().chainId(id).el(el).expectedVersion(expected).build();
	}

	private void await(Check check) {
		long deadline = System.currentTimeMillis() + 5000;
		while (System.currentTimeMillis() < deadline) {
			if (check.done()) { return; }
			try { Thread.sleep(20); }
			catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
		}
		throw new AssertionError("condition not met");
	}

	private interface Check { boolean done(); }
}
