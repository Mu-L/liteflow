package com.yomahub.liteflow.publisher;

import com.yomahub.liteflow.repository.vo.ChangeRecord;
import com.yomahub.liteflow.publisher.exception.PublisherConfigurationException;
import com.yomahub.liteflow.publisher.exception.PublisherProviderNotFoundException;
import com.yomahub.liteflow.publisher.exception.RuleValidationException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

class RulePublisherFactoryTest {

	@Test
	void rejectsMissingProvider() {
		Assertions.assertThrows(PublisherProviderNotFoundException.class,
				() -> RulePublisherFactory.create(new TestConfig("app", PublisherBackend.ETCD)));
	}

	@Test
	void expectedVersionSemanticsAreRepresentable() {
		Assertions.assertNull(PublishChainRequest.builder()
				.chainId("c").el("THEN(a)").build().getExpectedVersion());
		Assertions.assertEquals(Long.valueOf(0L), PublishChainRequest.builder()
				.chainId("c").el("THEN(a)").expectedVersion(0L).build().getExpectedVersion());
		Assertions.assertEquals(Long.valueOf(7L), RemoveRuleRequest.builder()
				.targetId("c").expectedVersion(7L).build().getExpectedVersion());
	}

	@Test
	void validatesConfigurationBeforeInspectingProviders() {
		StubProvider provider = new StubProvider(PublisherBackend.SQL);

		Assertions.assertThrows(PublisherConfigurationException.class,
				() -> RulePublisherFactory.create(new TestConfig(" ", PublisherBackend.SQL),
						Collections.singletonList(provider)));

		Assertions.assertEquals(0, provider.supportCalls);
	}

	@Test
	void selectsExactlyOneSupportingProvider() {
		StubProvider redis = new StubProvider(PublisherBackend.REDIS);
		StubProvider sql = new StubProvider(PublisherBackend.SQL);
		RulePublisher publisher = RulePublisherFactory.create(
				new TestConfig("app", PublisherBackend.SQL), Arrays.asList(redis, sql));
		PublishChainRequest request = PublishChainRequest.builder()
				.chainId("c1").el("THEN(a)").expectedVersion(0L).build();

		PublishResult result = publisher.publishChain(request);

		Assertions.assertSame(request, sql.publisher.lastChainRequest);
		Assertions.assertEquals("c1", result.getTargetId());
		Assertions.assertEquals(0, redis.createCalls);
		Assertions.assertEquals(1, sql.createCalls);
		publisher.close();
		Assertions.assertEquals(1, sql.publisher.closeCalls);
	}

	@Test
	void rejectsMultipleSupportingProvidersWithoutCreatingEither() {
		StubProvider first = new StubProvider(PublisherBackend.SQL);
		StubProvider second = new StubProvider(PublisherBackend.SQL);

		PublisherConfigurationException error = Assertions.assertThrows(
				PublisherConfigurationException.class,
				() -> RulePublisherFactory.create(new TestConfig("app", PublisherBackend.SQL),
						Arrays.asList(first, second)));

		Assertions.assertTrue(error.getMessage().contains(first.getClass().getName()));
		Assertions.assertEquals(0, first.createCalls);
		Assertions.assertEquals(0, second.createCalls);
	}

	@Test
	void validatesRequestsBeforeCallingBackend() {
		Assertions.assertThrows(RuleValidationException.class,
				() -> PublishChainRequest.builder().chainId(" ").el("THEN(a)").build());
		Assertions.assertThrows(RuleValidationException.class,
				() -> PublishChainRequest.builder().chainId("c").el(" ").build());
		Assertions.assertThrows(RuleValidationException.class,
				() -> PublishScriptRequest.builder().nodeId("s").script("return true")
						.type("common").build());
		Assertions.assertThrows(RuleValidationException.class,
				() -> RemoveRuleRequest.builder().targetId("c").expectedVersion(-1L).build());

		StubProvider provider = new StubProvider(PublisherBackend.SQL);
		RulePublisher publisher = RulePublisherFactory.create(new TestConfig("app", PublisherBackend.SQL),
				Collections.singletonList(provider));
		Assertions.assertThrows(RuleValidationException.class, () -> publisher.publishChain(null));
		Assertions.assertEquals(0, provider.publisher.publishCalls);
	}

	@Test
	void publishResultExposesCommittedPosition() {
		PublishResult result = PublishResult.builder()
				.targetId("s1")
				.targetType(ChangeRecord.TargetType.SCRIPT)
				.operation(ChangeRecord.Op.UPSERT)
				.version(3L)
				.sequence(19L)
				.build();

		Assertions.assertEquals("s1", result.getTargetId());
		Assertions.assertEquals(ChangeRecord.TargetType.SCRIPT, result.getTargetType());
		Assertions.assertEquals(ChangeRecord.Op.UPSERT, result.getOperation());
		Assertions.assertEquals(3L, result.getVersion());
		Assertions.assertEquals(19L, result.getSequence());
	}

	private static final class TestConfig implements RulePublisherConfig {

		private final String applicationName;
		private final PublisherBackend backend;

		private TestConfig(String applicationName, PublisherBackend backend) {
			this.applicationName = applicationName;
			this.backend = backend;
		}

		@Override
		public String applicationName() {
			return applicationName;
		}

		@Override
		public PublisherBackend backend() {
			return backend;
		}
	}

	private static final class StubProvider implements RulePublisherProvider {

		private final PublisherBackend backend;
		private final StubPublisher publisher = new StubPublisher();
		private int supportCalls;
		private int createCalls;

		private StubProvider(PublisherBackend backend) {
			this.backend = backend;
		}

		@Override
		public boolean supports(RulePublisherConfig config) {
			supportCalls++;
			return config.backend() == backend;
		}

		@Override
		public RulePublisher create(RulePublisherConfig config) {
			createCalls++;
			return publisher;
		}
	}

	private static final class StubPublisher implements RulePublisher {

		private PublishChainRequest lastChainRequest;
		private int publishCalls;
		private int closeCalls;

		@Override
		public PublishResult publishChain(PublishChainRequest request) {
			publishCalls++;
			lastChainRequest = request;
			return PublishResult.builder()
					.targetId(request.getChainId())
					.targetType(ChangeRecord.TargetType.CHAIN)
					.operation(ChangeRecord.Op.UPSERT)
					.version(1L)
					.sequence(1L)
					.build();
		}

		@Override
		public PublishResult publishScript(PublishScriptRequest request) {
			publishCalls++;
			return null;
		}

		@Override
		public PublishResult removeChain(RemoveRuleRequest request) {
			publishCalls++;
			return null;
		}

		@Override
		public PublishResult removeScript(RemoveRuleRequest request) {
			publishCalls++;
			return null;
		}

		@Override
		public void close() {
			closeCalls++;
		}
	}
}
