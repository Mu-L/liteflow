package com.yomahub.liteflow.repository.etcd;

import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.PublishResult;
import com.yomahub.liteflow.publisher.PublishScriptRequest;
import com.yomahub.liteflow.publisher.RemoveRuleRequest;
import com.yomahub.liteflow.publisher.exception.PublisherConfigurationException;
import com.yomahub.liteflow.publisher.exception.RuleStorageException;
import com.yomahub.liteflow.publisher.exception.VersionConflictException;
import com.yomahub.liteflow.repository.RuleChangeListener;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import com.yomahub.liteflow.repository.vo.RuleManifest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EtcdMockContractTest {

	private FakeKv kv;
	private EtcdKeys keys;
	private EtcdRecordCodec codec;
	private EtcdRulePublisher publisher;
	private EtcdRuleRepository repository;

	@BeforeEach
	void setUp() {
		kv = new FakeKv();
		keys = new EtcdKeys("/lf", "app");
		codec = new EtcdRecordCodec();
		publisher = new EtcdRulePublisher(kv, keys, codec);
		repository = new EtcdRuleRepository(kv, keys, codec);
	}

	@AfterEach
	void tearDown() {
		publisher.close();
	}

	@Test
	void publishesUpdatesReadsAndDeletesChainsAtomically() {
		PublishResult created = publisher.publishChain(chain("c1", "THEN(a)", 0L));
		PublishResult updated = publisher.publishChain(PublishChainRequest.builder()
				.chainId("c1").el("THEN(b)").route("AND(b)").namespace("ns")
				.expectedVersion(created.getVersion()).build());

		assertEquals(1, created.getVersion());
		assertEquals(2, updated.getVersion());
		assertTrue(updated.getSequence() > created.getSequence());
		assertEquals("THEN(b)", repository.fetchChain("c1").getEl());
		assertEquals("AND(b)", repository.fetchChain("c1").getRoute());
		assertEquals("ns", repository.fetchChain("c1").getNamespace());

		PublishResult removed = publisher.removeChain(RemoveRuleRequest.builder()
				.targetId("c1").expectedVersion(updated.getVersion()).build());
		assertEquals(ChangeRecord.Op.DELETE, removed.getOperation());
		assertNull(repository.fetchChain("c1"));
		assertNull(kv.raw(keys.chainMeta("c1")));
		assertNull(kv.raw(keys.chainContent("c1")));
	}

	@Test
	void preservesScriptMetadataAcrossRepositoryReads() {
		publisher.publishScript(PublishScriptRequest.builder().nodeId("s1")
				.script("return true").name("guard").type("boolean_script")
				.language("groovy").expectedVersion(0L).build());

		assertEquals("return true", repository.fetchScript("s1").getScript());
		assertEquals("guard", repository.fetchScriptMeta("s1").getName());
		assertEquals("boolean_script", repository.fetchScriptMeta("s1").getType());
		assertEquals("groovy", repository.fetchScriptMeta("s1").getLanguage());
	}

	@Test
	void versionConflictLeavesBothKeysUnchanged() {
		publisher.publishChain(chain("c1", "THEN(a)", 0L));
		String metadata = kv.raw(keys.chainMeta("c1"));
		String content = kv.raw(keys.chainContent("c1"));

		assertThrows(VersionConflictException.class,
				() -> publisher.publishChain(chain("c1", "THEN(b)", 7L)));
		assertEquals(metadata, kv.raw(keys.chainMeta("c1")));
		assertEquals(content, kv.raw(keys.chainContent("c1")));
	}

	@Test
	void unconditionalPublishRetriesACompetingTransaction() {
		kv.failNextTransaction();
		PublishResult result = publisher.publishChain(chain("c1", "THEN(a)", null));
		assertEquals(1, result.getVersion());
		assertNotNull(repository.fetchChain("c1"));
	}

	@Test
	void manifestUsesMetadataRangesAndDetectsMissingContent() {
		publisher.publishChain(chain("c1", "THEN(a)", 0L));
		publisher.publishScript(PublishScriptRequest.builder().nodeId("s1")
				.script("return 1").type("script").expectedVersion(0L).build());

		RuleManifest manifest = repository.fetchManifest();
		assertEquals(1, manifest.getChains().size());
		assertEquals(1, manifest.getScripts().size());
		assertEquals(2, kv.rangePrefixes().size());

		kv.removeDirect(keys.chainContent("c1"));
		assertThrows(RuleStorageException.class, () -> repository.fetchChain("c1"));
	}

	@Test
	void watchBuffersBeforeActivationAndDeliversUpdatesAndDeletes() {
		FakeWatch watch = new FakeWatch();
		EtcdWatchChangeSource source = new EtcdWatchChangeSource(watch, keys, codec);
		List<ChangeRecord> received = new ArrayList<>();
		AtomicInteger reconciles = new AtomicInteger();
		source.open(new RuleChangeListener() {
			@Override public void onChanges(List<ChangeRecord> changes) { received.addAll(changes); }
			@Override public void onReconcileRequired() { reconciles.incrementAndGet(); }
		});
		try {
			publisher.publishChain(chain("c1", "THEN(a)", 0L));
			watch.emitPut(keys.chainMeta("c1"), kv.raw(keys.chainMeta("c1")), 3);
			assertTrue(received.isEmpty());

			source.activate(0);
			await(() -> received.size() == 1);
			watch.emitDelete(keys.chainMeta("c1"), 4);
			await(() -> received.size() == 2);

			assertEquals(ChangeRecord.Op.UPSERT, received.get(0).getOp());
			assertEquals(ChangeRecord.Op.DELETE, received.get(1).getOp());
			assertEquals(4, source.health().getCursor());
			assertEquals(0, reconciles.get());
			assertFalse(source.requiresContinuousSequence());
		}
		finally {
			source.close();
		}
	}

	@Test
	void providerRejectsMissingClientAndBlankRootWithoutConnecting() {
		EtcdRulePublisherProvider provider = new EtcdRulePublisherProvider();
		assertTrue(provider.supports(EtcdPublisherConfig.builder().applicationName("app").build()));
		assertThrows(PublisherConfigurationException.class,
				() -> provider.create(EtcdPublisherConfig.builder().applicationName("app").build()));
		assertThrows(PublisherConfigurationException.class,
				() -> provider.create(EtcdPublisherConfig.builder().applicationName("app")
						.endpoints("http://mock.invalid").rootPath(" ").build()));
	}

	private PublishChainRequest chain(String id, String el, Long expected) {
		return PublishChainRequest.builder().chainId(id).el(el).expectedVersion(expected).build();
	}

	private void await(Check check) {
		long deadline = System.currentTimeMillis() + 2000;
		while (System.currentTimeMillis() < deadline) {
			if (check.done()) { return; }
			try { Thread.sleep(10); }
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				break;
			}
		}
		throw new AssertionError("condition not met");
	}

	private interface Check { boolean done(); }

	private static final class FakeKv implements EtcdKvFacade {
		private final Map<String, Stored> values = new LinkedHashMap<>();
		private final List<String> rangePrefixes = new ArrayList<>();
		private long revision;
		private boolean failNextTransaction;

		@Override public synchronized Value get(String key) {
			Stored stored = values.get(key);
			return stored == null ? new Value(null, 0, revision)
					: new Value(stored.value, stored.modRevision, revision);
		}

		@Override public synchronized Range range(String prefix) {
			rangePrefixes.add(prefix);
			List<Entry> entries = new ArrayList<>();
			List<String> sortedKeys = new ArrayList<>(values.keySet());
			Collections.sort(sortedKeys);
			for (String key : sortedKeys) {
				if (key.startsWith(prefix)) {
					Stored value = values.get(key);
					entries.add(new Entry(key, value.value, value.modRevision));
				}
			}
			return new Range(entries, revision);
		}

		@Override public synchronized TxnResult putPair(String metadataKey, long expectedModRevision,
				String contentKey, String content, String metadata) {
			if (failNextTransaction) {
				failNextTransaction = false;
				return new TxnResult(false, revision);
			}
			Stored current = values.get(metadataKey);
			long actual = current == null ? 0 : current.modRevision;
			if (actual != expectedModRevision) { return new TxnResult(false, revision); }
			long next = ++revision;
			values.put(contentKey, new Stored(content, next));
			values.put(metadataKey, new Stored(metadata, next));
			return new TxnResult(true, next);
		}

		@Override public synchronized TxnResult deletePair(String metadataKey, long expectedModRevision,
				String contentKey) {
			Stored current = values.get(metadataKey);
			long actual = current == null ? 0 : current.modRevision;
			if (actual != expectedModRevision) { return new TxnResult(false, revision); }
			long next = ++revision;
			values.remove(metadataKey);
			values.remove(contentKey);
			return new TxnResult(true, next);
		}

		private synchronized String raw(String key) {
			Stored value = values.get(key);
			return value == null ? null : value.value;
		}
		private synchronized void removeDirect(String key) { values.remove(key); revision++; }
		private List<String> rangePrefixes() { return rangePrefixes; }
		private void failNextTransaction() { failNextTransaction = true; }

		private static final class Stored {
			private final String value;
			private final long modRevision;
			private Stored(String value, long modRevision) {
				this.value = value;
				this.modRevision = modRevision;
			}
		}
	}

	private static final class FakeWatch implements EtcdWatchFacade {
		private final List<Registration> registrations = new ArrayList<>();
		@Override public synchronized Handle watch(String prefix, long startRevision, Listener listener) {
			Registration registration = new Registration(prefix, startRevision, listener);
			registrations.add(registration);
			return () -> registration.closed = true;
		}
		private synchronized void emitPut(String key, String value, long revision) {
			emit(new Event(key, value, revision, false));
		}
		private synchronized void emitDelete(String key, long revision) {
			emit(new Event(key, null, revision, true));
		}
		private void emit(Event event) {
			for (Registration registration : new ArrayList<>(registrations)) {
				if (!registration.closed && event.key().startsWith(registration.prefix)
						&& (registration.startRevision == 0 || event.revision() >= registration.startRevision)) {
					registration.listener.onEvents(Collections.singletonList(event));
				}
			}
		}
		private static final class Registration {
			private final String prefix;
			private final long startRevision;
			private final Listener listener;
			private boolean closed;
			private Registration(String prefix, long startRevision, Listener listener) {
				this.prefix = prefix;
				this.startRevision = startRevision;
				this.listener = listener;
			}
		}
	}
}
