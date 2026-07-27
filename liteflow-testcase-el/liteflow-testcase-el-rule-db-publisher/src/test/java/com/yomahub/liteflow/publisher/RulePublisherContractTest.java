package com.yomahub.liteflow.publisher;

import com.yomahub.liteflow.publisher.exception.PublisherConfigurationException;
import com.yomahub.liteflow.publisher.exception.PublisherProviderNotFoundException;
import com.yomahub.liteflow.publisher.exception.RuleValidationException;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RulePublisherContractTest {

	@Test
	void selectsOneProviderAndDelegatesAllOperations() {
		StubProvider redis = new StubProvider(PublisherBackend.REDIS);
		StubProvider sql = new StubProvider(PublisherBackend.SQL);
		RulePublisher publisher = RulePublisherFactory.create(
				new TestConfig("app", PublisherBackend.SQL), Arrays.asList(redis, sql));

		PublishChainRequest chain = PublishChainRequest.builder()
				.chainId("c1").el("THEN(a)").expectedVersion(0L).build();
		PublishScriptRequest script = PublishScriptRequest.builder()
				.nodeId("s1").script("return true").type("boolean_script").build();
		RemoveRuleRequest remove = RemoveRuleRequest.builder().targetId("c1").build();
		publisher.publishChain(chain);
		publisher.publishScript(script);
		publisher.removeChain(remove);
		publisher.removeScript(remove);

		assertSame(chain, sql.publisher.lastChainRequest);
		assertEquals(4, sql.publisher.calls);
		assertEquals(0, redis.createCalls);
		assertEquals(1, sql.createCalls);
		publisher.close();
		assertEquals(1, sql.publisher.closeCalls);
	}

	@Test
	void validatesConfigurationBeforeInspectingProviders() {
		StubProvider provider = new StubProvider(PublisherBackend.SQL);
		assertThrows(PublisherConfigurationException.class,
				() -> RulePublisherFactory.create(new TestConfig(" ", PublisherBackend.SQL),
						Collections.singletonList(provider)));
		assertEquals(0, provider.supportCalls);
	}

	@Test
	void rejectsMissingAndAmbiguousProviders() {
		assertThrows(PublisherProviderNotFoundException.class,
				() -> RulePublisherFactory.create(new TestConfig("app", PublisherBackend.ETCD),
						Collections.emptyList()));

		StubProvider first = new StubProvider(PublisherBackend.SQL);
		StubProvider second = new StubProvider(PublisherBackend.SQL);
		PublisherConfigurationException error = assertThrows(PublisherConfigurationException.class,
				() -> RulePublisherFactory.create(new TestConfig("app", PublisherBackend.SQL),
						Arrays.asList(first, second)));
		assertTrue(error.getMessage().contains("multiple RulePublisherProvider"));
		assertEquals(0, first.createCalls);
		assertEquals(0, second.createCalls);
	}

	@Test
	void rejectsNullPublisherReturnedByProvider() {
		RulePublisherProvider provider = new RulePublisherProvider() {
			@Override public boolean supports(RulePublisherConfig config) { return true; }
			@Override public RulePublisher create(RulePublisherConfig config) { return null; }
		};
		assertThrows(PublisherConfigurationException.class,
				() -> RulePublisherFactory.create(new TestConfig("app", PublisherBackend.SQL),
						Collections.singletonList(provider)));
	}

	@Test
	void validatesRequestsBeforeCallingBackend() {
		assertThrows(RuleValidationException.class,
				() -> PublishChainRequest.builder().chainId(" ").el("THEN(a)").build());
		assertThrows(RuleValidationException.class,
				() -> PublishChainRequest.builder().chainId("c").el(" ").build());
		assertThrows(RuleValidationException.class,
				() -> PublishScriptRequest.builder().nodeId("s").script("return true").type("common").build());
		assertThrows(RuleValidationException.class,
				() -> RemoveRuleRequest.builder().targetId("c").expectedVersion(-1L).build());

		StubProvider provider = new StubProvider(PublisherBackend.SQL);
		RulePublisher publisher = RulePublisherFactory.create(new TestConfig("app", PublisherBackend.SQL),
				Collections.singletonList(provider));
		assertThrows(RuleValidationException.class, () -> publisher.publishChain(null));
		assertEquals(0, provider.publisher.calls);
	}

	@Test
	void representsOptionalAndExactExpectedVersions() {
		assertNull(PublishChainRequest.builder().chainId("c").el("THEN(a)").build().getExpectedVersion());
		assertEquals(Long.valueOf(0L), PublishChainRequest.builder()
				.chainId("c").el("THEN(a)").expectedVersion(0L).build().getExpectedVersion());
		assertEquals(Long.valueOf(7L), RemoveRuleRequest.builder()
				.targetId("c").expectedVersion(7L).build().getExpectedVersion());
	}

	@Test
	void publishResultExposesCommittedPosition() {
		PublishResult result = PublishResult.builder().targetId("s1")
				.targetType(ChangeRecord.TargetType.SCRIPT).operation(ChangeRecord.Op.UPSERT)
				.version(3L).sequence(19L).build();
		assertEquals("s1", result.getTargetId());
		assertEquals(ChangeRecord.TargetType.SCRIPT, result.getTargetType());
		assertEquals(ChangeRecord.Op.UPSERT, result.getOperation());
		assertEquals(3L, result.getVersion());
		assertEquals(19L, result.getSequence());
	}

	private static final class TestConfig implements RulePublisherConfig {
		private final String applicationName;
		private final PublisherBackend backend;
		private TestConfig(String applicationName, PublisherBackend backend) {
			this.applicationName = applicationName;
			this.backend = backend;
		}
		@Override public String applicationName() { return applicationName; }
		@Override public PublisherBackend backend() { return backend; }
	}

	private static final class StubProvider implements RulePublisherProvider {
		private final PublisherBackend backend;
		private final StubPublisher publisher = new StubPublisher();
		private int supportCalls;
		private int createCalls;
		private StubProvider(PublisherBackend backend) { this.backend = backend; }
		@Override public boolean supports(RulePublisherConfig config) {
			supportCalls++;
			return config.backend() == backend;
		}
		@Override public RulePublisher create(RulePublisherConfig config) {
			createCalls++;
			return publisher;
		}
	}

	private static final class StubPublisher implements RulePublisher {
		private PublishChainRequest lastChainRequest;
		private int calls;
		private int closeCalls;
		@Override public PublishResult publishChain(PublishChainRequest request) {
			calls++;
			lastChainRequest = request;
			return result(request.getChainId(), ChangeRecord.TargetType.CHAIN, ChangeRecord.Op.UPSERT);
		}
		@Override public PublishResult publishScript(PublishScriptRequest request) {
			calls++;
			return result(request.getNodeId(), ChangeRecord.TargetType.SCRIPT, ChangeRecord.Op.UPSERT);
		}
		@Override public PublishResult removeChain(RemoveRuleRequest request) {
			calls++;
			return result(request.getTargetId(), ChangeRecord.TargetType.CHAIN, ChangeRecord.Op.DELETE);
		}
		@Override public PublishResult removeScript(RemoveRuleRequest request) {
			calls++;
			return result(request.getTargetId(), ChangeRecord.TargetType.SCRIPT, ChangeRecord.Op.DELETE);
		}
		@Override public void close() { closeCalls++; }
		private PublishResult result(String id, ChangeRecord.TargetType type, ChangeRecord.Op operation) {
			return PublishResult.builder().targetId(id).targetType(type).operation(operation)
					.version(1L).sequence(1L).build();
		}
	}
}
