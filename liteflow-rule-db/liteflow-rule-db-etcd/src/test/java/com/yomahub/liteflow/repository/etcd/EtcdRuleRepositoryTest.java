package com.yomahub.liteflow.repository.etcd;

import com.yomahub.liteflow.publisher.exception.RuleStorageException;
import com.yomahub.liteflow.repository.vo.ChainRecord;
import com.yomahub.liteflow.repository.vo.RuleManifest;
import com.yomahub.liteflow.repository.vo.ScriptRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EtcdRuleRepositoryTest {

	private FakeEtcdKvFacade fake;
	private EtcdKeys keys;
	private EtcdRecordCodec codec;
	private EtcdRuleRepository repository;

	@BeforeEach
	void setUp() {
		fake = new FakeEtcdKvFacade();
		keys = new EtcdKeys("/lf", "app");
		codec = new EtcdRecordCodec();
		repository = new EtcdRuleRepository(fake, keys, codec);
	}

	@Test
	void manifestRangesOnlyMetadataPrefixes() {
		fake.putDirect(keys.chainMeta("c1"), chainMeta(1, "m1"));
		fake.putDirect(keys.chainContent("c1"), codec.encodeContent(1, "THEN(a)"));
		fake.putDirect(keys.scriptMeta("s1"), scriptMeta(1, "sm1"));
		fake.putDirect(keys.scriptContent("s1"), codec.encodeContent(1, "return 1"));

		RuleManifest manifest = repository.fetchManifest();

		assertEquals(Arrays.asList(keys.chainMetaPrefix(), keys.scriptMetaPrefix()), fake.rangePrefixes());
		assertTrue(fake.exactReads().isEmpty());
		assertEquals(1, manifest.getChains().size());
		assertEquals(1, manifest.getScripts().size());
		assertEquals(4, manifest.getLatestSeq());
	}

	@Test
	void fetchChainRejectsMetadataContentVersionMismatch() {
		fake.putDirect(keys.chainMeta("c1"), chainMeta(2, "m2"));
		fake.putDirect(keys.chainContent("c1"), codec.encodeContent(1, "THEN(a)"));

		assertThrows(RuleStorageException.class, () -> repository.fetchChain("c1"));
	}

	@Test
	void fetchesBodyOnlyForRequestedTarget() {
		fake.putDirect(keys.chainMeta("c1"), chainMeta(1, "m1"));
		fake.putDirect(keys.chainContent("c1"), codec.encodeContent(1, "THEN(a)"));

		assertEquals("THEN(a)", repository.fetchChain("c1").getEl());
		assertEquals(Arrays.asList(keys.chainMeta("c1"), keys.chainContent("c1"), keys.chainMeta("c1")),
				fake.exactReads());
	}

	private String chainMeta(long version, String md5) {
		ChainRecord record = new ChainRecord();
		record.setVersion(version);
		record.setMd5(md5);
		record.setEnable(true);
		return codec.encodeChainMeta(record);
	}

	private String scriptMeta(long version, String md5) {
		ScriptRecord record = new ScriptRecord();
		record.setVersion(version);
		record.setMd5(md5);
		record.setType("script");
		record.setEnable(true);
		return codec.encodeScriptMeta(record);
	}
}
