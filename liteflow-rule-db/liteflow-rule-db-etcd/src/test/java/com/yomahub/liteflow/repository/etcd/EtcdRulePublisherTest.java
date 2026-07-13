package com.yomahub.liteflow.repository.etcd;

import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.PublishResult;
import com.yomahub.liteflow.publisher.PublishScriptRequest;
import com.yomahub.liteflow.publisher.RemoveRuleRequest;
import com.yomahub.liteflow.publisher.exception.VersionConflictException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EtcdRulePublisherTest {

	private FakeEtcdKvFacade fake;
	private EtcdKeys keys;
	private EtcdRecordCodec codec;
	private EtcdRulePublisher publisher;

	@BeforeEach
	void setUp() {
		fake = new FakeEtcdKvFacade();
		keys = new EtcdKeys("/lf", "app");
		codec = new EtcdRecordCodec();
		publisher = new EtcdRulePublisher(fake, keys, codec);
	}

	@Test
	void publishesChainAndAdvancesBusinessVersion() {
		PublishResult created = publisher.publishChain(chain("c1", "THEN(a)", null));
		PublishResult updated = publisher.publishChain(chain("c1", "THEN(b)", 1L));

		assertEquals(1, created.getVersion());
		assertEquals(2, updated.getVersion());
		assertTrue(updated.getSequence() > created.getSequence());
		assertEquals("THEN(b)", codec.decodeChain("c1", fake.raw(keys.chainMeta("c1")),
				fake.raw(keys.chainContent("c1"))).getEl());
	}

	@Test
	void conflictDoesNotWriteEitherKey() {
		publisher.publishChain(chain("c1", "THEN(a)", 0L));
		String metadata = fake.raw(keys.chainMeta("c1"));
		String content = fake.raw(keys.chainContent("c1"));

		assertThrows(VersionConflictException.class,
				() -> publisher.publishChain(chain("c1", "THEN(b)", 7L)));
		assertEquals(metadata, fake.raw(keys.chainMeta("c1")));
		assertEquals(content, fake.raw(keys.chainContent("c1")));
	}

	@Test
	void unconditionalPublishRetriesLostCas() {
		fake.failNextTransaction();
		PublishResult result = publisher.publishChain(chain("c1", "THEN(a)", null));
		assertEquals(1, result.getVersion());
		assertNotNull(fake.raw(keys.chainMeta("c1")));
	}

	@Test
	void publishesAndDeletesScriptAtomically() {
		PublishResult created = publisher.publishScript(PublishScriptRequest.builder()
				.nodeId("s1").script("return 1").type("script").language("groovy")
				.expectedVersion(0L).build());
		PublishResult deleted = publisher.removeScript(RemoveRuleRequest.builder()
				.targetId("s1").expectedVersion(created.getVersion()).build());

		assertEquals(1, deleted.getVersion());
		assertNull(fake.raw(keys.scriptMeta("s1")));
		assertNull(fake.raw(keys.scriptContent("s1")));
	}

	private PublishChainRequest chain(String id, String el, Long expected) {
		return PublishChainRequest.builder().chainId(id).el(el).expectedVersion(expected).build();
	}
}
