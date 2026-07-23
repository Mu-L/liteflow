package com.yomahub.liteflow.repository.zk;

import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.repository.ChangeSourceHealth;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Resilience tests for the ZooKeeper watch-based change source against a real
 * ZooKeeper server (curator-test TestingServer): connection loss, session
 * expiry, reconnect reconciliation, concurrent CAS publishes and idempotent close.
 */
class ZkWatchResilienceTest {

	private TestingServer server;
	private CuratorFramework client;
	private ZkPaths paths;
	private ZkRecordCodec codec;

	@BeforeEach
	void setUp() throws Exception {
		server = ZkTestSupport.server();
		client = ZkTestSupport.client(server, 1500);
		paths = new ZkPaths("/lf", "resilience");
		codec = new ZkRecordCodec();
		ZkTestSupport.createRoots(client, paths);
	}

	@AfterEach
	void tearDown() throws Exception {
		if (client != null) { client.close(); }
		if (server != null) { server.close(); }
	}

	@Test
	void suspendedAndLostDegradeHealthAndReconnectRequestsReconcile() throws Exception {
		ZkTestSupport.RecordingListener listener = new ZkTestSupport.RecordingListener();
		ZkCacheChangeSource source = new ZkCacheChangeSource(client, paths, codec);
		source.open(listener);
		source.activate(0);
		try {
			ZkRulePublisher publisher = new ZkRulePublisher(client, paths, codec);
			publisher.publishChain(chain("c1", "THEN(a)"));
			ZkTestSupport.await("initial delivery",
					() -> listener.countFor("c1") == 1);
			assertEquals(ChangeSourceHealth.Status.UP, source.health().getStatus());
			int reconcilesBeforeOutage = listener.reconciles.get();

			server.stop();
			ZkTestSupport.await("health degrades while server is down", () -> {
				ChangeSourceHealth health = source.health();
				return health.getStatus() == ChangeSourceHealth.Status.DEGRADED
						&& health.getRecentError() != null
						&& health.getRecentError().contains("connection");
			});
			// keep the server down past the negotiated session timeout so the
			// session expires and Curator reports LOST, then bring it back
			Thread.sleep(3000);
			server.restart();

			ZkTestSupport.await("reconnect requests reconcile",
					() -> listener.reconciles.get() > reconcilesBeforeOutage);
		}
		finally {
			source.close();
		}
	}

	@Test
	void watchesSelfHealAfterSessionExpiryAndStillDeliver() throws Exception {
		ZkTestSupport.RecordingListener listener = new ZkTestSupport.RecordingListener();
		ZkCacheChangeSource source = new ZkCacheChangeSource(client, paths, codec);
		source.open(listener);
		source.activate(0);
		try {
			server.stop();
			Thread.sleep(3000);
			server.restart();
			int reconcilesBefore = listener.reconciles.get();
			ZkTestSupport.await("reconnect requests reconcile",
					() -> listener.reconciles.get() > reconcilesBefore);

			ZkRulePublisher publisher = new ZkRulePublisher(client, paths, codec);
			publisher.publishChain(chain("c2", "THEN(b)"));
			ZkTestSupport.await("delivery after session expiry",
					() -> listener.changes.stream().anyMatch(change ->
							change.getTargetId().equals("c2") && change.getOp() == ChangeRecord.Op.UPSERT));
			ZkTestSupport.await("health recovers",
					() -> source.health().getStatus() == ChangeSourceHealth.Status.UP);
		}
		finally {
			source.close();
		}
	}

	@Test
	void concurrentPublishersRetryCasConflicts() throws Exception {
		ZkRulePublisher first = new ZkRulePublisher(client, paths, codec);
		CuratorFramework secondClient = ZkTestSupport.client(server, 5000);
		try {
			ZkRulePublisher second = new ZkRulePublisher(secondClient, paths, codec);
			first.publishChain(chain("hot", "THEN(a)"));

			ExecutorService pool = Executors.newFixedThreadPool(2);
			List<Callable<Object>> tasks = new ArrayList<>();
			for (int i = 0; i < 3; i++) {
				int round = i;
				tasks.add(() -> { first.publishChain(chain("hot", "THEN(x" + round + ")")); return null; });
				tasks.add(() -> { second.publishChain(chain("hot", "THEN(y" + round + ")")); return null; });
			}
			for (Future<Object> future : pool.invokeAll(tasks)) {
				future.get();
			}
			pool.shutdownNow();

			ZkRuleRepository repository = new ZkRuleRepository(client, paths, codec);
			assertEquals(7, repository.fetchChainMeta("hot").getVersion());
		}
		finally {
			secondClient.close();
		}
	}

	@Test
	void closeIsIdempotentAndFurtherCallsAreIgnored() throws Exception {
		ZkTestSupport.RecordingListener listener = new ZkTestSupport.RecordingListener();
		ZkCacheChangeSource source = new ZkCacheChangeSource(client, paths, codec);
		source.open(listener);
		source.activate(0);
		source.close();
		source.close();

		assertEquals(ChangeSourceHealth.Status.DOWN, source.health().getStatus());
		source.activate(0);
		source.onReconciled(1);
		assertEquals(ChangeSourceHealth.Status.DOWN, source.health().getStatus());

		ZkRulePublisher publisher = new ZkRulePublisher(client, paths, codec);
		publisher.close();
		publisher.close();
		assertTrue(client.getZookeeperClient().isConnected());
	}

	private PublishChainRequest chain(String id, String el) {
		return PublishChainRequest.builder().chainId(id).el(el).build();
	}
}
