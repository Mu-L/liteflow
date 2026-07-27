package com.yomahub.liteflow.repository.zk;

import com.yomahub.liteflow.repository.RuleChangeListener;
import com.yomahub.liteflow.repository.vo.ChainRecord;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.data.Stat;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.fail;

/** Shared helpers for real-ZooKeeper tests backed by curator-test TestingServer. */
final class ZkTestSupport {

	private ZkTestSupport() {
	}

	static TestingServer server() throws Exception {
		File dataDir = Files.createTempDirectory("zk-test").toFile();
		InstanceSpec spec = new InstanceSpec(dataDir, -1, -1, -1, true, -1, 250, 64);
		return new TestingServer(spec, true);
	}

	static CuratorFramework client(TestingServer server, int sessionTimeoutMs) throws InterruptedException {
		CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(),
				sessionTimeoutMs, 3000, new RetryOneTime(50));
		client.start();
		if (!client.blockUntilConnected(10, TimeUnit.SECONDS)) {
			client.close();
			throw new IllegalStateException("timed out connecting to the ZooKeeper test server");
		}
		return client;
	}

	static void createRoots(CuratorFramework client, ZkPaths paths) throws Exception {
		client.createContainers(paths.chainMetaRoot());
		client.createContainers(paths.chainContentRoot());
		client.createContainers(paths.scriptMetaRoot());
		client.createContainers(paths.scriptContentRoot());
	}

	static void await(String description, BooleanSupplier condition) throws InterruptedException {
		long deadline = System.currentTimeMillis() + 15000;
		while (System.currentTimeMillis() < deadline) {
			if (condition.getAsBoolean()) { return; }
			Thread.sleep(50);
		}
		fail("timed out waiting for: " + description);
	}

	static ChildData childData(String path, long mzxid, byte[] data) {
		Stat stat = new Stat();
		stat.setMzxid(mzxid);
		stat.setVersion(1);
		return new ChildData(path, stat, data);
	}

	static ChainRecord chainRecord(String chainId, long version, String el) {
		ChainRecord record = new ChainRecord();
		record.setChainId(chainId);
		record.setVersion(version);
		record.setMd5("md5-" + version);
		record.setEnable(true);
		record.setEl(el);
		return record;
	}

	/** Thread-safe listener recording delivered changes and reconcile requests. */
	static final class RecordingListener implements RuleChangeListener {

		final List<ChangeRecord> changes = new CopyOnWriteArrayList<>();
		final AtomicInteger reconciles = new AtomicInteger();
		private final AtomicInteger failures = new AtomicInteger();

		@Override
		public void onChanges(List<ChangeRecord> delivered) {
			if (failures.get() > 0) {
				failures.decrementAndGet();
				throw new RuntimeException("injected listener failure");
			}
			changes.addAll(delivered);
		}

		@Override
		public void onReconcileRequired() {
			reconciles.incrementAndGet();
		}

		void failNextDeliveries(int count) {
			failures.addAndGet(count);
		}

		long countFor(String targetId) {
			return changes.stream().filter(change -> change.getTargetId().equals(targetId)).count();
		}
	}
}
