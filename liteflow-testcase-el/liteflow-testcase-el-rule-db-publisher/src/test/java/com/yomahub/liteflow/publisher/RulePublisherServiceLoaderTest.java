package com.yomahub.liteflow.publisher;

import com.yomahub.liteflow.publisher.exception.PublisherConfigurationException;
import com.yomahub.liteflow.publisher.exception.PublisherProviderNotFoundException;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ServiceConfigurationError;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the one-argument {@link RulePublisherFactory#create(RulePublisherConfig)}
 * which resolves providers through the real {@link java.util.ServiceLoader} and the
 * {@code META-INF/services} registration of this test module.
 */
class RulePublisherServiceLoaderTest {

	@Test
	void loadsProviderThroughServiceLoaderAndDelegates() {
		RulePublisher publisher = RulePublisherFactory.create(config("svc-normal"));

		PublishChainRequest chain = PublishChainRequest.builder().chainId("c1").el("THEN(a)").build();
		PublishResult result = publisher.publishChain(chain);

		assertEquals("c1", result.getTargetId());
		assertSame(chain, StubProviderA.lastPublisher.lastChainRequest);
		publisher.close();
		assertEquals(1, StubProviderA.lastPublisher.closeCalls);
	}

	@Test
	void throwsProviderNotFoundWhenNoRegisteredProviderSupportsConfig() {
		assertThrows(PublisherProviderNotFoundException.class,
				() -> RulePublisherFactory.create(config("svc-unknown")));
	}

	@Test
	void throwsConfigurationExceptionWhenMultipleRegisteredProvidersMatch() {
		PublisherConfigurationException error = assertThrows(PublisherConfigurationException.class,
				() -> RulePublisherFactory.create(config("svc-conflict")));
		assertTrue(error.getMessage().contains("multiple RulePublisherProvider"));
	}

	@Test
	void wrapsBrokenProviderRegistrationIntoConfigurationException() throws Exception {
		Path dir = Files.createTempDirectory("broken-publisher-spi");
		Path services = Files.createDirectories(dir.resolve("META-INF/services"));
		Files.write(services.resolve("com.yomahub.liteflow.publisher.RulePublisherProvider"),
				"com.example.DoesNotExistProvider\n".getBytes(StandardCharsets.UTF_8));

		ClassLoader brokenLoader = new URLClassLoader(new URL[] { dir.toUri().toURL() },
				getClass().getClassLoader());
		ClassLoader originalLoader = Thread.currentThread().getContextClassLoader();
		Thread.currentThread().setContextClassLoader(brokenLoader);
		try {
			PublisherConfigurationException error = assertThrows(PublisherConfigurationException.class,
					() -> RulePublisherFactory.create(config("svc-normal")));
			assertTrue(error.getCause() instanceof ServiceConfigurationError);
		}
		finally {
			Thread.currentThread().setContextClassLoader(originalLoader);
		}
	}

	private static RulePublisherConfig config(String applicationName) {
		return new RulePublisherConfig() {
			@Override
			public String applicationName() {
				return applicationName;
			}

			@Override
			public PublisherBackend backend() {
				return PublisherBackend.SQL;
			}
		};
	}

	/** Registered provider resolving {@code svc-normal} and {@code svc-conflict}. */
	public static final class StubProviderA implements RulePublisherProvider {

		private static StubPublisher lastPublisher;

		@Override
		public boolean supports(RulePublisherConfig config) {
			return "svc-normal".equals(config.applicationName())
					|| "svc-conflict".equals(config.applicationName());
		}

		@Override
		public RulePublisher create(RulePublisherConfig config) {
			lastPublisher = new StubPublisher();
			return lastPublisher;
		}
	}

	/** Second registered provider only matching {@code svc-conflict}. */
	public static final class StubProviderB implements RulePublisherProvider {

		@Override
		public boolean supports(RulePublisherConfig config) {
			return "svc-conflict".equals(config.applicationName());
		}

		@Override
		public RulePublisher create(RulePublisherConfig config) {
			return new StubPublisher();
		}
	}

	private static final class StubPublisher implements RulePublisher {

		private PublishChainRequest lastChainRequest;
		private int closeCalls;

		@Override
		public PublishResult publishChain(PublishChainRequest request) {
			lastChainRequest = request;
			return PublishResult.builder().targetId(request.getChainId())
					.targetType(ChangeRecord.TargetType.CHAIN).operation(ChangeRecord.Op.UPSERT)
					.version(1L).sequence(1L).build();
		}

		@Override
		public PublishResult publishScript(PublishScriptRequest request) {
			return PublishResult.builder().targetId(request.getNodeId())
					.targetType(ChangeRecord.TargetType.SCRIPT).operation(ChangeRecord.Op.UPSERT)
					.version(1L).sequence(1L).build();
		}

		@Override
		public PublishResult removeChain(RemoveRuleRequest request) {
			return PublishResult.builder().targetId(request.getTargetId())
					.targetType(ChangeRecord.TargetType.CHAIN).operation(ChangeRecord.Op.DELETE)
					.version(1L).sequence(1L).build();
		}

		@Override
		public PublishResult removeScript(RemoveRuleRequest request) {
			return PublishResult.builder().targetId(request.getTargetId())
					.targetType(ChangeRecord.TargetType.SCRIPT).operation(ChangeRecord.Op.DELETE)
					.version(1L).sequence(1L).build();
		}

		@Override
		public void close() {
			closeCalls++;
		}
	}
}
