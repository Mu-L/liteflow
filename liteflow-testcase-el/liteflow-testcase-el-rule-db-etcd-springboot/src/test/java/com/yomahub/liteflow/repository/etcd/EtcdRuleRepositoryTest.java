package com.yomahub.liteflow.repository.etcd;

import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.PublishScriptRequest;
import com.yomahub.liteflow.publisher.exception.RuleStorageException;
import com.yomahub.liteflow.repository.vo.RuleManifest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Repository read-path contracts, including the manifest snapshot
 * consistency guarantee (second range pinned to the first read's revision).
 */
class EtcdRuleRepositoryTest {

	private FakeEtcdKv kv;
	private EtcdKeys keys;
	private EtcdRecordCodec codec;
	private EtcdRuleRepository repository;
	private EtcdRulePublisher publisher;

	@BeforeEach
	void setUp() {
		kv = new FakeEtcdKv();
		keys = new EtcdKeys("/lf", "app");
		codec = new EtcdRecordCodec();
		repository = new EtcdRuleRepository(kv, keys, codec);
		publisher = new EtcdRulePublisher(kv, keys, codec);
	}

	@Test
	void manifestIsAConsistentSnapshotAcrossBothPrefixes() {
		publisher.publishChain(PublishChainRequest.builder().chainId("c1").el("THEN(a)")
				.expectedVersion(0L).build());
		publisher.publishScript(PublishScriptRequest.builder().nodeId("s1").script("return 1")
				.type("script").expectedVersion(0L).build());
		long snapshotRevision = kv.currentRevision();

		// a concurrent write lands between the chain range and the script range
		kv.onceOnRange(keys.chainMetaPrefix(), () -> publisher.publishScript(
				PublishScriptRequest.builder().nodeId("s2").script("return 2")
						.type("script").expectedVersion(0L).build()));

		RuleManifest manifest = repository.fetchManifest();
		assertEquals(1, manifest.getChains().size());
		assertEquals(1, manifest.getScripts().size());
		assertEquals("s1", manifest.getScripts().get(0).getNodeId());
		assertEquals(snapshotRevision, manifest.getLatestSeq());
	}

	@Test
	void manifestSkipsDisabledEntries() {
		publisher.publishChain(PublishChainRequest.builder().chainId("c1").el("THEN(a)")
				.expectedVersion(0L).build());
		kv.putDirect(keys.scriptMeta("off"), "{\"version\":1,\"md5\":\"m\",\"enable\":false}");

		RuleManifest manifest = repository.fetchManifest();
		assertEquals(1, manifest.getChains().size());
		assertEquals(0, manifest.getScripts().size());
	}

	@Test
	void manifestFailsOnCorruptedEntry() {
		kv.putDirect(keys.scriptMeta("broken"), "not-json");
		assertThrows(RuleStorageException.class, () -> repository.fetchManifest());
	}

	@Test
	void fetchChainDetectsConcurrentMetaChange() {
		publisher.publishChain(PublishChainRequest.builder().chainId("c1").el("THEN(a)")
				.expectedVersion(0L).build());
		// mutates the meta key between the first and the second meta read
		kv.onceOnGet(keys.chainContent("c1"), () -> publisher.publishChain(
				PublishChainRequest.builder().chainId("c1").el("THEN(b)")
						.expectedVersion(1L).build()));

		assertThrows(RuleStorageException.class, () -> repository.fetchChain("c1"));
	}

	@Test
	void fetchScriptDetectsConcurrentMetaChange() {
		publisher.publishScript(PublishScriptRequest.builder().nodeId("s1").script("return 1")
				.type("script").expectedVersion(0L).build());
		kv.onceOnGet(keys.scriptContent("s1"), () -> publisher.publishScript(
				PublishScriptRequest.builder().nodeId("s1").script("return 2")
						.type("script").expectedVersion(1L).build()));

		assertThrows(RuleStorageException.class, () -> repository.fetchScript("s1"));
	}

	@Test
	void fetchScriptFailsWhenContentIsMissing() {
		publisher.publishScript(PublishScriptRequest.builder().nodeId("s1").script("return 1")
				.type("script").expectedVersion(0L).build());
		kv.removeDirect(keys.scriptContent("s1"));
		assertThrows(RuleStorageException.class, () -> repository.fetchScript("s1"));
	}

	@Test
	void metaReadsReturnNullForMissingOrDisabledEntries() {
		assertNull(repository.fetchChainMeta("nope"));
		assertNull(repository.fetchScriptMeta("nope"));
		kv.putDirect(keys.chainMeta("off"), "{\"version\":1,\"md5\":\"m\",\"enable\":false}");
		kv.putDirect(keys.scriptMeta("off"), "{\"version\":1,\"md5\":\"m\",\"enable\":false}");
		assertNull(repository.fetchChainMeta("off"));
		assertNull(repository.fetchScriptMeta("off"));
		assertNull(repository.fetchChain("off"));
		assertNull(repository.fetchScript("off"));
	}

	@Test
	void manifestIncludesChainAndScriptMetadata() {
		publisher.publishChain(PublishChainRequest.builder().chainId("c1").el("THEN(a)")
				.route("AND(a)").namespace("ns").expectedVersion(0L).build());
		publisher.publishScript(PublishScriptRequest.builder().nodeId("s1").script("return 1")
				.name("guard").type("boolean_script").language("groovy").expectedVersion(0L).build());

		RuleManifest manifest = repository.fetchManifest();
		assertEquals(1, manifest.getChains().size());
		assertEquals(1, manifest.getScripts().size());
		assertEquals(1, manifest.getChains().get(0).getVersion());
		assertTrue(manifest.getLatestSeq() >= 2);
	}
}
