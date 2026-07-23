package com.yomahub.liteflow.repository.redis;

import com.yomahub.liteflow.exception.SeqGapException;
import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.PublishResult;
import com.yomahub.liteflow.publisher.PublishScriptRequest;
import com.yomahub.liteflow.publisher.RemoveRuleRequest;
import com.yomahub.liteflow.publisher.RulePublisher;
import com.yomahub.liteflow.publisher.exception.PublisherConfigurationException;
import com.yomahub.liteflow.publisher.exception.RuleStorageException;
import com.yomahub.liteflow.publisher.exception.VersionConflictException;
import com.yomahub.liteflow.repository.ChangeSourceHealth;
import com.yomahub.liteflow.repository.RuleChangeListener;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import org.junit.jupiter.api.Test;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@SuppressWarnings({ "unchecked", "rawtypes" })
class RedisMockContractTest {

	@Test
	void providerValidatesConfigurationWithoutOpeningRedis() {
		RedisRulePublisherProvider provider = new RedisRulePublisherProvider();
		RedissonClient client = mock(RedissonClient.class);
		RedisPublisherConfig config = config(client);

		assertTrue(provider.supports(config));
		try (RulePublisher ignored = provider.create(config)) {
			verify(client, never()).getScript(StringCodec.INSTANCE);
		}
		verify(client, never()).shutdown();

		assertThrows(PublisherConfigurationException.class,
				() -> provider.create(RedisPublisherConfig.builder().applicationName("app").build()));
		assertThrows(PublisherConfigurationException.class,
				() -> provider.create(RedisPublisherConfig.builder().applicationName("app")
						.address("redis://mock.invalid").keyPrefix("bad prefix").build()));
	}

	@Test
	void luaResultMapsCreateUpdateAndDeletePositions() {
		RedissonClient client = mock(RedissonClient.class);
		RScript script = script(client);
		doReturn(Arrays.asList(1L, 7L), Arrays.asList(2L, 8L), Arrays.asList(2L, 9L))
				.when(script).eval(any(RScript.Mode.class), anyString(), any(RScript.ReturnType.class),
						anyList(), (Object[]) any());

		try (RulePublisher publisher = publisher(client)) {
			PublishResult created = publisher.publishChain(chain("c1", "THEN(a)", 0L));
			PublishResult updated = publisher.publishChain(chain("c1", "THEN(b)", 1L));
			PublishResult deleted = publisher.removeChain(RemoveRuleRequest.builder()
					.targetId("c1").expectedVersion(2L).build());

			assertEquals(1, created.getVersion());
			assertEquals(7, created.getSequence());
			assertEquals(2, updated.getVersion());
			assertEquals(8, updated.getSequence());
			assertEquals(ChangeRecord.Op.DELETE, deleted.getOperation());
			assertEquals(9, deleted.getSequence());
		}
		verify(client, never()).shutdown();
	}

	@Test
	void scriptMetadataIsPassedThroughLuaPublisher() {
		RedissonClient client = mock(RedissonClient.class);
		RScript script = script(client);
		doReturn(Arrays.asList(1L, 4L)).when(script).eval(any(RScript.Mode.class), anyString(),
				any(RScript.ReturnType.class), anyList(), (Object[]) any());

		PublishResult result;
		try (RulePublisher publisher = publisher(client)) {
			result = publisher.publishScript(PublishScriptRequest.builder().nodeId("s1")
					.script("return true").name("guard").type("boolean_script")
					.language("groovy").expectedVersion(0L).build());
		}
		assertEquals(ChangeRecord.TargetType.SCRIPT, result.getTargetType());
		assertEquals(1, result.getVersion());
	}

	@Test
	void negativeVersionResultBecomesConflict() {
		RedissonClient client = mock(RedissonClient.class);
		RScript script = script(client);
		doReturn(Arrays.asList(-1L, 3L)).when(script).eval(any(RScript.Mode.class), anyString(),
				any(RScript.ReturnType.class), anyList(), (Object[]) any());

		try (RulePublisher publisher = publisher(client)) {
			VersionConflictException error = assertThrows(VersionConflictException.class,
					() -> publisher.publishChain(chain("c1", "THEN(b)", 2L)));
			assertTrue(error.getMessage().contains("current version is[3]"));
		}
	}

	@Test
	void incompleteAndFailedLuaResultsAreStorageErrors() {
		RedissonClient incompleteClient = mock(RedissonClient.class);
		RScript incomplete = script(incompleteClient);
		doReturn(Collections.singletonList(1L)).when(incomplete).eval(any(RScript.Mode.class), anyString(),
				any(RScript.ReturnType.class), anyList(), (Object[]) any());
		try (RulePublisher publisher = publisher(incompleteClient)) {
			assertThrows(RuleStorageException.class,
					() -> publisher.publishChain(chain("c1", "THEN(a)", 0L)));
		}

		RedissonClient failedClient = mock(RedissonClient.class);
		RScript failed = script(failedClient);
		doThrow(new RuntimeException("redis unavailable")).when(failed).eval(any(RScript.Mode.class), anyString(),
				any(RScript.ReturnType.class), anyList(), (Object[]) any());
		try (RulePublisher publisher = publisher(failedClient)) {
			RuleStorageException error = assertThrows(RuleStorageException.class,
					() -> publisher.publishChain(chain("c1", "THEN(a)", 0L)));
			assertTrue(error.getMessage().contains("redis unavailable"));
		}
	}

	@Test
	void pollingDeliversChangesAndAdvancesHealthCursor() {
		ChangeRecord one = change(1);
		ChangeRecord two = change(2);
		StubRepository repository = new StubRepository(2, Arrays.asList(one, two), null);
		List<ChangeRecord> received = new java.util.ArrayList<>();
		RedisPollingChangeSource source = source(repository, received::addAll);
		try {
			source.pollOnce();
			assertEquals(2, received.size());
			assertEquals(2, source.health().getCursor());
			assertEquals(ChangeSourceHealth.Status.UP, source.health().getStatus());
		}
		finally { source.close(); }
	}

	@Test
	void pollingGapRequestsReconcileWithoutAdvancingCursor() {
		AtomicInteger reconciles = new AtomicInteger();
		StubRepository repository = new StubRepository(3, Collections.emptyList(),
				new SeqGapException("trimmed"));
		RedisPollingChangeSource source = source(repository, new RuleChangeListener() {
			@Override public void onChanges(List<ChangeRecord> changes) { }
			@Override public void onReconcileRequired() { reconciles.incrementAndGet(); }
		});
		try {
			source.pollOnce();
			assertEquals(1, reconciles.get());
			assertEquals(0, source.health().getCursor());
			assertEquals(ChangeSourceHealth.Status.DEGRADED, source.health().getStatus());
		}
		finally { source.close(); }
	}

	private RScript script(RedissonClient client) {
		RScript script = mock(RScript.class);
		doReturn(script).when(client).getScript(StringCodec.INSTANCE);
		return script;
	}

	private RulePublisher publisher(RedissonClient client) {
		return new RedisRulePublisherProvider().create(config(client));
	}

	private RedisPublisherConfig config(RedissonClient client) {
		return RedisPublisherConfig.builder().applicationName("app")
				.keyPrefix("lf").redissonClient(client).build();
	}

	private RedisPollingChangeSource source(RedisRuleRepository repository, RuleChangeListener listener) {
		RedisPollingChangeSource source = new RedisPollingChangeSource(repository, 3600);
		source.open(listener);
		source.activate(0);
		return source;
	}

	private ChangeRecord change(long seq) {
		return new ChangeRecord(seq, ChangeRecord.TargetType.CHAIN,
				"c" + seq, ChangeRecord.Op.UPSERT, 1);
	}

	private PublishChainRequest chain(String id, String el, Long expected) {
		return PublishChainRequest.builder().chainId(id).el(el).expectedVersion(expected).build();
	}

	private static final class StubRepository extends RedisRuleRepository {
		private final long latest;
		private final List<ChangeRecord> changes;
		private final RuntimeException failure;
		private StubRepository(long latest, List<ChangeRecord> changes, RuntimeException failure) {
			super(null, new RedisKeys("lf", "app"));
			this.latest = latest;
			this.changes = changes;
			this.failure = failure;
		}
		@Override public long fetchLatestSeq() { return latest; }
		@Override public List<ChangeRecord> fetchChangesSince(long seq) {
			if (failure != null) { throw failure; }
			return changes;
		}
	}
}
