package com.yomahub.liteflow.repository.etcd;

import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.PublishResult;
import com.yomahub.liteflow.publisher.PublishScriptRequest;
import com.yomahub.liteflow.publisher.RemoveRuleRequest;
import com.yomahub.liteflow.publisher.exception.RuleStorageException;
import com.yomahub.liteflow.publisher.exception.VersionConflictException;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Conflict, retry and removal paths of the etcd publisher. */
class EtcdRulePublisherConflictTest {

	private FakeEtcdKv kv;
	private EtcdKeys keys;
	private EtcdRulePublisher publisher;

	@BeforeEach
	void setUp() {
		kv = new FakeEtcdKv();
		keys = new EtcdKeys("/lf", "app");
		publisher = new EtcdRulePublisher(kv, keys, new EtcdRecordCodec());
	}

	@Test
	void removeOfMissingRuleIsAnIdempotentDelete() {
		PublishResult result = publisher.removeChain(RemoveRuleRequest.builder().targetId("gone").build());
		assertEquals(ChangeRecord.Op.DELETE, result.getOperation());
		assertEquals(0, result.getVersion());
		assertTrue(result.getSequence() > 0);
	}

	@Test
	void removeWithStaleExpectedVersionConflicts() {
		publisher.publishChain(chain("c1", "THEN(a)", 0L));
		assertThrows(VersionConflictException.class, () -> publisher.removeChain(
				RemoveRuleRequest.builder().targetId("c1").expectedVersion(9L).build()));
		assertThrows(VersionConflictException.class, () -> publisher.removeChain(
				RemoveRuleRequest.builder().targetId("c1").expectedVersion(0L).build()));
		assertTrue(kv.raw(keys.chainMeta("c1")) != null);
	}

	@Test
	void conditionalPublishConflictsAfterCompetingTransaction() {
		publisher.publishChain(chain("c1", "THEN(a)", 0L));
		kv.failNextTransactions(1);
		assertThrows(VersionConflictException.class,
				() -> publisher.publishChain(chain("c1", "THEN(b)", 1L)));
		assertEquals("THEN(a)", new EtcdRuleRepository(kv, keys, new EtcdRecordCodec())
				.fetchChain("c1").getEl());
	}

	@Test
	void unconditionalPublishGivesUpAfterRetryLimit() {
		kv.failNextTransactions(8);
		RuleStorageException failure = assertThrows(RuleStorageException.class,
				() -> publisher.publishChain(chain("c1", "THEN(a)", null)));
		assertTrue(failure.getMessage().contains("retry limit"));
	}

	@Test
	void unconditionalRemoveRetriesCompetingTransaction() {
		publisher.publishScript(PublishScriptRequest.builder().nodeId("s1").script("return 1")
				.type("script").expectedVersion(0L).build());
		kv.failNextTransactions(1);
		PublishResult result = publisher.removeScript(RemoveRuleRequest.builder().targetId("s1").build());
		assertEquals(ChangeRecord.Op.DELETE, result.getOperation());
		assertEquals(1, result.getVersion());
		assertNull(kv.raw(keys.scriptMeta("s1")));
		assertNull(kv.raw(keys.scriptContent("s1")));
	}

	@Test
	void unconditionalRemoveGivesUpAfterRetryLimit() {
		publisher.publishChain(chain("c1", "THEN(a)", 0L));
		kv.failNextTransactions(8);
		RuleStorageException failure = assertThrows(RuleStorageException.class,
				() -> publisher.removeChain(RemoveRuleRequest.builder().targetId("c1").build()));
		assertTrue(failure.getMessage().contains("retry limit"));
	}

	@Test
	void conditionalRemoveConflictsAfterCompetingTransaction() {
		publisher.publishChain(chain("c1", "THEN(a)", 0L));
		kv.failNextTransactions(1);
		assertThrows(VersionConflictException.class, () -> publisher.removeChain(
				RemoveRuleRequest.builder().targetId("c1").expectedVersion(1L).build()));
	}

	private PublishChainRequest chain(String id, String el, Long expected) {
		return PublishChainRequest.builder().chainId(id).el(el).expectedVersion(expected).build();
	}
}
