package com.yomahub.liteflow.repository.etcd;

import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.PublishResult;
import com.yomahub.liteflow.publisher.PublishScriptRequest;
import com.yomahub.liteflow.publisher.RemoveRuleRequest;
import com.yomahub.liteflow.publisher.RulePublisher;
import com.yomahub.liteflow.publisher.exception.VersionConflictException;
import com.yomahub.liteflow.repository.RuleChangeListener;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import com.yomahub.liteflow.repository.vo.RuleManifest;
import io.etcd.jetcd.Client;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Real etcd transaction, watch, and compaction coverage. */
@Testcontainers(disabledWithoutDocker = true)
class EtcdContainerIntegrationTest {

	@Container
	private static final GenericContainer<?> ETCD = new GenericContainer<>(
			DockerImageName.parse("gcr.io/etcd-development/etcd:v3.5.21"))
			.withCommand("/usr/local/bin/etcd", "--name", "rule-db", "--data-dir", "/tmp/etcd-data",
					"--listen-client-urls", "http://0.0.0.0:2379",
					"--advertise-client-urls", "http://0.0.0.0:2379",
					"--listen-peer-urls", "http://0.0.0.0:2380")
			.withExposedPorts(2379)
			.waitingFor(Wait.forListeningPort());

	private Client client;

	@BeforeEach
	void setUp() {
		client = Client.builder().endpoints(endpoint()).build();
	}

	@AfterEach
	void tearDown() {
		if (client != null) { client.close(); }
	}

	@Test
	void publisherRepositoryAndUnifiedRootWatchWorkAgainstRealEtcd() {
		EtcdKeys keys = new EtcdKeys("/liteflow", "it-app");
		EtcdRecordCodec codec = new EtcdRecordCodec();
		EtcdRuleRepository repository = new EtcdRuleRepository(
				new JetcdKvFacade(client.getKVClient()), keys, codec);
		EtcdWatchChangeSource source = new EtcdWatchChangeSource(
				new JetcdWatchFacade(client.getWatchClient()), keys, codec);
		RecordingListener listener = new RecordingListener();
		source.open(listener);
		RuleManifest baseline = repository.fetchManifest();
		source.activate(baseline.getLatestSeq());

		EtcdPublisherConfig config = EtcdPublisherConfig.builder().applicationName("it-app")
				.rootPath("/liteflow").client(client).build();
		try (RulePublisher publisher = new EtcdRulePublisherProvider().create(config)) {
			PublishResult chain = publisher.publishChain(PublishChainRequest.builder()
					.chainId("c1").el("THEN(a)").expectedVersion(0L).build());
			publisher.publishScript(PublishScriptRequest.builder().nodeId("s1").script("return 1")
					.name("script").type("script").language("groovy").expectedVersion(0L).build());
			assertTrue(chain.getSequence() > baseline.getLatestSeq());
			assertThrows(VersionConflictException.class, () -> publisher.publishChain(
					PublishChainRequest.builder().chainId("c1").el("THEN(x)").expectedVersion(0L).build()));
		}

		Await.until(() -> listener.changes.size() >= 2, 5000);
		assertEquals(ChangeRecord.TargetType.CHAIN, listener.changes.get(0).getTargetType());
		assertEquals(ChangeRecord.TargetType.SCRIPT, listener.changes.get(1).getTargetType());
		RuleManifest manifest = repository.fetchManifest();
		assertEquals(1, manifest.getChains().size());
		assertEquals(1, manifest.getScripts().size());
		assertEquals("THEN(a)", repository.fetchChain("c1").getEl());

		try (RulePublisher publisher = new EtcdRulePublisherProvider().create(config)) {
			publisher.removeChain(RemoveRuleRequest.builder().targetId("c1").expectedVersion(1L).build());
		}
		Await.until(() -> listener.changes.stream().anyMatch(change -> change.getOp() == ChangeRecord.Op.DELETE), 5000);
		assertNull(repository.fetchChain("c1"));
		source.close();
	}

	@Test
	void watchFromCompactedRevisionRequestsReconciliation() throws Exception {
		EtcdKeys keys = new EtcdKeys("/liteflow", "compact-app");
		EtcdRecordCodec codec = new EtcdRecordCodec();
		EtcdPublisherConfig config = EtcdPublisherConfig.builder().applicationName("compact-app")
				.rootPath("/liteflow").client(client).build();
		long revision;
		try (RulePublisher publisher = new EtcdRulePublisherProvider().create(config)) {
			revision = publisher.publishChain(PublishChainRequest.builder()
					.chainId("c1").el("THEN(a)").build()).getSequence();
		}
		client.getKVClient().compact(revision).get();

		EtcdWatchChangeSource source = new EtcdWatchChangeSource(
				new JetcdWatchFacade(client.getWatchClient()), keys, codec);
		RecordingListener listener = new RecordingListener();
		source.open(listener);
		source.activate(0);
		source.onReconciled(0);
		try {
			Await.until(() -> listener.reconciles.get() > 0, 5000);
			assertTrue(source.health().getLastError().toLowerCase().contains("compact"));
		}
		finally {
			source.close();
		}
	}

	private String endpoint() {
		return "http://" + ETCD.getHost() + ":" + ETCD.getMappedPort(2379);
	}

	private static final class RecordingListener implements RuleChangeListener {
		private final List<ChangeRecord> changes = new CopyOnWriteArrayList<>();
		private final AtomicInteger reconciles = new AtomicInteger();

		@Override
		public void onChanges(List<ChangeRecord> delivered) {
			changes.addAll(delivered);
		}

		@Override
		public void onReconcileRequired() {
			reconciles.incrementAndGet();
		}
	}
}
