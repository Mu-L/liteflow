package com.yomahub.liteflow.repository.nacos;

import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.repository.ChangeSourceHealth;
import com.yomahub.liteflow.repository.RuleChangeListener;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NacosListenerChangeSourceTest {

	private FakeNacosConfig facade;
	private NacosRulePublisher publisher;
	private NacosRuleRepository repository;
	private NacosListenerChangeSource source;
	private List<ChangeRecord> received;
	private AtomicInteger reconciles;

	@BeforeEach
	void setUp() {
		facade = new FakeNacosConfig();
		NacosConfigKey key = new NacosConfigKey("lf", "app", "RULES");
		NacosCatalogCodec codec = new NacosCatalogCodec();
		NacosCatalogStore store = new NacosCatalogStore(facade, key, 1000, codec);
		publisher = new NacosRulePublisher(facade, key, codec, 1000);
		repository = new NacosRuleRepository(store);
		source = new NacosListenerChangeSource(store);
		received = new CopyOnWriteArrayList<>();
		reconciles = new AtomicInteger();
	}

	@Test
	void buffersStartupWindowThenDeliversContinuousChanges() {
		source.open(listener());
		long baseline = repository.fetchManifest().getLatestSeq();
		publisher.publishChain(chain("c1"));
		assertTrue(received.isEmpty());

		source.activate(baseline);
		await(() -> received.size() == 1);
		publisher.publishChain(PublishChainRequest.builder().chainId("c1")
				.el("THEN(b)").expectedVersion(1L).build());
		await(() -> received.size() == 2);

		assertEquals(1, received.get(0).getSeq());
		assertEquals(2, received.get(1).getSeq());
		assertEquals(ChangeSourceHealth.Status.UP, source.health().getStatus());
		assertEquals(2, source.health().getCursor());
		assertTrue(source.requiresContinuousSequence());
		source.close();
	}

	@Test
	void missedNotificationRequestsReconcileAndResumesFromNewBaseline() {
		source.open(listener());
		source.activate(0);
		facade.dropNextNotification();
		publisher.publishChain(chain("c1"));
		publisher.publishChain(chain("c2"));

		await(() -> reconciles.get() == 1);
		assertTrue(received.isEmpty());
		assertEquals(ChangeSourceHealth.Status.DEGRADED, source.health().getStatus());
		assertTrue(source.health().getRecentError().contains("sequence gap"));

		long baseline = repository.fetchManifest().getLatestSeq();
		source.onReconciled(baseline);
		publisher.publishChain(chain("c3"));
		await(() -> received.size() == 1);
		assertEquals(3, received.get(0).getSeq());
		assertEquals(ChangeSourceHealth.Status.UP, source.health().getStatus());
		source.close();
	}

	@Test
	void corruptCallbackAndConsumerFailureDegradeWithoutAdvancingCursor() {
		source.open(new RuleChangeListener() {
			@Override
			public void onChanges(List<ChangeRecord> changes) {
				throw new RuntimeException("consumer failed");
			}

			@Override
			public void onReconcileRequired() {
				reconciles.incrementAndGet();
			}
		});
		source.activate(0);
		facade.emit("not-json");
		await(() -> reconciles.get() == 1);
		assertEquals(0, source.health().getCursor());
		source.onReconciled(0);

		publisher.publishChain(chain("c1"));
		await(() -> reconciles.get() == 2);
		assertEquals(0, source.health().getCursor());
		assertTrue(source.health().getRecentError().contains("consumer failed"));
		source.close();
	}

	@Test
	void validatesArgumentsAndCloseIsIdempotent() {
		assertThrows(IllegalArgumentException.class, () -> source.open(null));
		source.open(listener());
		assertEquals(1, facade.listenerCount());
		source.activate(0);
		source.close();
		source.close();
		assertEquals(0, facade.listenerCount());
		assertEquals(ChangeSourceHealth.Status.DOWN, source.health().getStatus());

		publisher.publishChain(chain("late"));
		assertTrue(received.isEmpty());
		assertFalse(source.health().getStatus() == ChangeSourceHealth.Status.UP);
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

	private PublishChainRequest chain(String id) {
		return PublishChainRequest.builder().chainId(id).el("THEN(a)").build();
	}

	private void await(Check check) {
		long deadline = System.currentTimeMillis() + 2000;
		while (System.currentTimeMillis() < deadline) {
			if (check.done()) { return; }
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
