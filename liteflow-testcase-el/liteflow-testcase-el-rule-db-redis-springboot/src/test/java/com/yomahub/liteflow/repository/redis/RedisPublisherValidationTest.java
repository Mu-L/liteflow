package com.yomahub.liteflow.repository.redis;

import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.PublishScriptRequest;
import com.yomahub.liteflow.publisher.RemoveRuleRequest;
import com.yomahub.liteflow.publisher.RulePublisher;
import com.yomahub.liteflow.publisher.exception.RuleValidationException;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/** The implementation validates requests even without the factory wrapper. */
class RedisPublisherValidationTest {

	private final RedissonClient client = mock(RedissonClient.class);

	@Test
	void publishChainRejectsKeyBreakingChainId() {
		try (RulePublisher publisher = publisher()) {
			PublishChainRequest request = PublishChainRequest.builder()
					.chainId("bad:id").el("THEN(a)").build();
			assertThrows(RuleValidationException.class, () -> publisher.publishChain(request));
		}
		verify(client, never()).getScript(StringCodec.INSTANCE);
	}

	@Test
	void publishChainRejectsNullAndOversizedNamespace() {
		try (RulePublisher publisher = publisher()) {
			assertThrows(RuleValidationException.class, () -> publisher.publishChain(null));

			StringBuilder namespace = new StringBuilder();
			for (int i = 0; i < 65; i++) {
				namespace.append('n');
			}
			PublishChainRequest request = PublishChainRequest.builder()
					.chainId("c1").el("THEN(a)").namespace(namespace.toString()).build();
			assertThrows(RuleValidationException.class, () -> publisher.publishChain(request));
		}
		verify(client, never()).getScript(StringCodec.INSTANCE);
	}

	@Test
	void publishScriptRejectsKeyBreakingNodeId() {
		try (RulePublisher publisher = publisher()) {
			PublishScriptRequest request = PublishScriptRequest.builder()
					.nodeId("bad id").script("return true").type("script").build();
			assertThrows(RuleValidationException.class, () -> publisher.publishScript(request));
			assertThrows(RuleValidationException.class, () -> publisher.publishScript(null));
		}
		verify(client, never()).getScript(StringCodec.INSTANCE);
	}

	@Test
	void removeRejectsKeyBreakingTargetId() {
		try (RulePublisher publisher = publisher()) {
			RemoveRuleRequest request = RemoveRuleRequest.builder().targetId("bad:id").build();
			assertThrows(RuleValidationException.class, () -> publisher.removeChain(request));
			assertThrows(RuleValidationException.class, () -> publisher.removeScript(request));
			assertThrows(RuleValidationException.class, () -> publisher.removeChain(null));
		}
		verify(client, never()).getScript(StringCodec.INSTANCE);
	}

	@Test
	void pollingSourceRejectsInvalidConstruction() {
		RedisRuleRepository repository = mock(RedisRuleRepository.class);
		assertThrows(ConfigErrorException.class, () -> new RedisPollingChangeSource(null, 1));
		assertThrows(ConfigErrorException.class, () -> new RedisPollingChangeSource(repository, 0));
		assertThrows(ConfigErrorException.class, () -> new RedisPollingChangeSource(repository, 1, 0));
		RedisPollingChangeSource source = new RedisPollingChangeSource(repository, 3600);
		try {
			assertThrows(IllegalArgumentException.class, () -> source.open(null));
		}
		finally {
			source.close();
		}
	}

	private RulePublisher publisher() {
		return new RedisRulePublisherProvider().create(RedisPublisherConfig.builder()
				.applicationName("app").keyPrefix("lf").redissonClient(client).build());
	}
}
