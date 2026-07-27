package com.yomahub.liteflow.repository.nacos;

import cn.hutool.crypto.SecureUtil;
import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.PublishResult;
import com.yomahub.liteflow.publisher.PublishScriptRequest;
import com.yomahub.liteflow.publisher.RemoveRuleRequest;
import com.yomahub.liteflow.publisher.exception.VersionConflictException;
import com.yomahub.liteflow.repository.vo.ChainRecord;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import com.yomahub.liteflow.repository.vo.RuleManifest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NacosMockContractTest {

	private FakeNacosConfig facade;
	private NacosRulePublisher publisher;
	private NacosRuleRepository repository;

	@BeforeEach
	void setUp() {
		facade = new FakeNacosConfig();
		NacosConfigKey key = new NacosConfigKey("lf", "app", "RULES");
		NacosCatalogCodec codec = new NacosCatalogCodec();
		publisher = new NacosRulePublisher(facade, key, codec, 1000);
		repository = new NacosRuleRepository(new NacosCatalogStore(facade, key, 1000, codec));
	}

	@Test
	void publishesUpdatesReadsAndDeletesOneAtomicCatalog() {
		PublishResult created = publisher.publishChain(chain("c1", "THEN(a)", 0L));
		PublishResult updated = publisher.publishChain(PublishChainRequest.builder().chainId("c1")
				.el("THEN(b)").route("AND(b)").namespace("ns")
				.expectedVersion(created.getVersion()).build());
		PublishResult script = publisher.publishScript(PublishScriptRequest.builder().nodeId("s1")
				.script("return true").name("guard").type("boolean_script")
				.language("groovy").expectedVersion(0L).build());

		assertEquals(1, created.getVersion());
		assertEquals(2, updated.getVersion());
		assertEquals(3, script.getSequence());
		RuleManifest manifest = repository.fetchManifest();
		assertEquals(1, manifest.getChains().size());
		assertEquals(1, manifest.getScripts().size());
		assertEquals("THEN(b)", repository.fetchChain("c1").getEl());
		assertEquals("AND(b)", repository.fetchChain("c1").getRoute());
		assertEquals("groovy", repository.fetchScriptMeta("s1").getLanguage());

		PublishResult removed = publisher.removeChain(RemoveRuleRequest.builder()
				.targetId("c1").expectedVersion(2L).build());
		assertEquals(ChangeRecord.Op.DELETE, removed.getOperation());
		assertEquals(2, removed.getVersion());
		assertEquals(4, removed.getSequence());
		assertNull(repository.fetchManifest().getChains().stream().findFirst().orElse(null));
		assertNull(repository.fetchChain("c1"));
	}

	@Test
	void staleExpectedVersionDoesNotChangeCatalogAndUnconditionalPublishRetries() {
		publisher.publishChain(chain("c1", "THEN(a)", 0L));
		String before = facade.content();

		assertThrows(VersionConflictException.class,
				() -> publisher.publishChain(chain("c1", "THEN(b)", 7L)));
		assertEquals(before, facade.content());

		facade.rejectNextCas();
		PublishResult updated = publisher.publishChain(chain("c1", "THEN(c)", null));
		assertEquals(2, updated.getVersion());
		assertEquals("THEN(c)", repository.fetchManifest().getChains().isEmpty()
				? null : repository.fetchChain("c1").getEl());
	}

	@Test
	void coldFetchAlwaysReadsTheAuthoritativeCatalog() {
		publisher.publishChain(chain("c1", "THEN(a)", 0L));
		assertEquals("THEN(a)", repository.fetchChain("c1").getEl());

		publisher.publishChain(chain("c1", "THEN(b)", 1L));

		assertEquals("THEN(b)", repository.fetchChain("c1").getEl());
	}

	@Test
	void concurrentCreatesWithExpectedZeroAllowExactlyOneCommit() throws Exception {
		ExecutorService executor = Executors.newFixedThreadPool(2);
		CountDownLatch ready = new CountDownLatch(2);
		CountDownLatch start = new CountDownLatch(1);
		try {
			Future<Object> first = executor.submit(() -> publishAtStart(ready, start));
			Future<Object> second = executor.submit(() -> publishAtStart(ready, start));
			ready.await();
			start.countDown();
			Object left = first.get();
			Object right = second.get();

			assertTrue(left instanceof PublishResult || right instanceof PublishResult);
			assertTrue(left instanceof VersionConflictException || right instanceof VersionConflictException);
			assertEquals(1, repository.fetchManifest().getLatestSeq());
			assertEquals(1, repository.fetchManifest().getChains().size());
		}
		finally {
			executor.shutdownNow();
		}
	}

	@Test
	void olderInFlightRefreshCannotBeMistakenForCatalogRollback() throws Exception {
		NacosCatalogCodec codec = new NacosCatalogCodec();
		String older = catalog(codec, "THEN(a)", 1);
		String newer = catalog(codec, "THEN(b)", 2);
		CountDownLatch readStarted = new CountDownLatch(1);
		CountDownLatch releaseRead = new CountDownLatch(1);
		NacosConfigFacade blocking = new NacosConfigFacade() {
			@Override
			public String get(String dataId, String group, long timeoutMillis) {
				readStarted.countDown();
				try {
					releaseRead.await();
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new IllegalStateException(e);
				}
				return older;
			}

			@Override
			public boolean publishCas(String dataId, String group, String content, String expectedMd5) {
				throw new UnsupportedOperationException();
			}

			@Override
			public Subscription subscribe(String dataId, String group, long timeoutMillis,
					Consumer<String> consumer) {
				throw new UnsupportedOperationException();
			}
		};
		NacosCatalogStore orderedStore = new NacosCatalogStore(blocking,
				new NacosConfigKey("lf", "ordered", "RULES"), 1000, codec);
		ExecutorService executor = Executors.newFixedThreadPool(2);
		CountDownLatch acceptStarted = new CountDownLatch(1);
		try {
			Future<NacosCatalog> refresh = executor.submit(orderedStore::refresh);
			assertTrue(readStarted.await(1, TimeUnit.SECONDS));
			Future<NacosCatalog> listener = executor.submit(() -> {
				acceptStarted.countDown();
				return orderedStore.accept(newer);
			});
			assertTrue(acceptStarted.await(1, TimeUnit.SECONDS));
			releaseRead.countDown();

			assertEquals(1, refresh.get().sequence());
			assertEquals(2, listener.get().sequence());
		}
		finally {
			releaseRead.countDown();
			executor.shutdownNow();
		}
	}

	private Object publishAtStart(CountDownLatch ready, CountDownLatch start) throws InterruptedException {
		ready.countDown();
		start.await();
		try {
			return publisher.publishChain(chain("same", "THEN(a)", 0L));
		}
		catch (VersionConflictException e) {
			return e;
		}
	}

	private PublishChainRequest chain(String id, String el, Long expected) {
		return PublishChainRequest.builder().chainId(id).el(el).expectedVersion(expected).build();
	}

	private String catalog(NacosCatalogCodec codec, String el, long version) {
		ChainRecord record = new ChainRecord();
		record.setChainId("c1");
		record.setEl(el);
		record.setVersion(version);
		record.setMd5(SecureUtil.md5(el));
		record.setEnable(true);
		ChangeRecord change = new ChangeRecord(version, ChangeRecord.TargetType.CHAIN,
				"c1", ChangeRecord.Op.UPSERT, version);
		return codec.encode(NacosCatalog.empty().withChain(record, change));
	}
}
