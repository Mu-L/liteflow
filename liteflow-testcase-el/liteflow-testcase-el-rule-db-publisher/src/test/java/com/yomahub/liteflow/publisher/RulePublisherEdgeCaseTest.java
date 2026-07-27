package com.yomahub.liteflow.publisher;

import com.yomahub.liteflow.enums.NodeTypeEnum;
import com.yomahub.liteflow.publisher.exception.PublisherConfigurationException;
import com.yomahub.liteflow.publisher.exception.RuleStorageException;
import com.yomahub.liteflow.publisher.exception.RuleValidationException;
import com.yomahub.liteflow.publisher.exception.VersionConflictException;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers defensive branches of {@link RulePublisherFactory}, the request builders and
 * the exception contracts that the contract test does not reach.
 */
class RulePublisherEdgeCaseTest {

	@Test
	void rejectsNullConfigAndNullBackend() {
		assertThrows(PublisherConfigurationException.class,
				() -> RulePublisherFactory.create(null, Collections.emptyList()));
		assertThrows(PublisherConfigurationException.class,
				() -> RulePublisherFactory.create(config("app", null), Collections.emptyList()));
	}

	@Test
	void skipsNullProvidersAndWrapsSupportsFailures() {
		RulePublisherProvider misbehaving = new RulePublisherProvider() {
			@Override
			public boolean supports(RulePublisherConfig config) {
				throw new IllegalStateException("broken provider");
			}

			@Override
			public RulePublisher create(RulePublisherConfig config) {
				throw new UnsupportedOperationException();
			}
		};

		PublisherConfigurationException error = assertThrows(PublisherConfigurationException.class,
				() -> RulePublisherFactory.create(config("app", PublisherBackend.SQL),
						Arrays.asList(null, misbehaving)));
		assertTrue(error.getMessage().contains("failed while inspecting config"));
		assertTrue(error.getCause() instanceof IllegalStateException);
	}

	@Test
	void wrapsUnexpectedProviderCreationFailure() {
		RulePublisherProvider provider = new RulePublisherProvider() {
			@Override public boolean supports(RulePublisherConfig config) { return true; }
			@Override public RulePublisher create(RulePublisherConfig config) {
				throw new IllegalStateException("cannot connect");
			}
		};

		PublisherConfigurationException error = assertThrows(PublisherConfigurationException.class,
				() -> RulePublisherFactory.create(config("app", PublisherBackend.SQL),
						Collections.singletonList(provider)));
		assertTrue(error.getMessage().contains("failed while creating publisher"));
		assertTrue(error.getCause() instanceof IllegalStateException);
	}

	@Test
	void validatingWrapperRejectsNullRequestsForEveryOperation() {
		RulePublisherProvider provider = new RulePublisherProvider() {
			@Override
			public boolean supports(RulePublisherConfig config) {
				return true;
			}

			@Override
			public RulePublisher create(RulePublisherConfig config) {
				return new NoOpPublisher();
			}
		};
		RulePublisher publisher = RulePublisherFactory.create(config("app", PublisherBackend.SQL),
				Collections.singletonList(provider));

		assertThrows(RuleValidationException.class, () -> publisher.publishChain(null));
		assertThrows(RuleValidationException.class, () -> publisher.publishScript(null));
		assertThrows(RuleValidationException.class, () -> publisher.removeChain(null));
		assertThrows(RuleValidationException.class, () -> publisher.removeScript(null));
	}

	@Test
	void validatingWrapperRejectsNullAndMismatchedResults() {
		RulePublisherProvider provider = new RulePublisherProvider() {
			@Override public boolean supports(RulePublisherConfig config) { return true; }
			@Override public RulePublisher create(RulePublisherConfig config) { return new NoOpPublisher(); }
		};
		RulePublisher publisher = RulePublisherFactory.create(config("app", PublisherBackend.SQL),
				Collections.singletonList(provider));

		assertThrows(RuleStorageException.class, () -> publisher.publishChain(
				PublishChainRequest.builder().chainId("c1").el("THEN(a)").build()));

		RulePublisher mismatched = RulePublisherFactory.create(config("app", PublisherBackend.SQL),
				Collections.singletonList(new ResultProvider()));
		assertThrows(RuleStorageException.class, () -> mismatched.removeChain(
				RemoveRuleRequest.builder().targetId("c1").build()));
	}

	@Test
	void publishResultRejectsIncompleteAndInvalidState() {
		assertThrows(IllegalStateException.class, () -> PublishResult.builder().build());
		assertThrows(IllegalStateException.class, () -> PublishResult.builder().targetId("c1")
				.targetType(ChangeRecord.TargetType.CHAIN).operation(ChangeRecord.Op.UPSERT)
				.version(-1).sequence(1).build());
		assertThrows(IllegalStateException.class, () -> PublishResult.builder().targetId("c1")
				.targetType(ChangeRecord.TargetType.CHAIN).operation(ChangeRecord.Op.UPSERT)
				.version(1).sequence(0).build());
	}

	@Test
	void chainRequestExposesRouteNamespaceAndRejectsNegativeVersion() {
		PublishChainRequest request = PublishChainRequest.builder().chainId("c1").el("THEN(a)")
				.route("r1").namespace("ns1").expectedVersion(2L).build();

		assertEquals("c1", request.getChainId());
		assertEquals("c1", request.getTargetId());
		assertEquals("THEN(a)", request.getEl());
		assertEquals("r1", request.getRoute());
		assertEquals("ns1", request.getNamespace());
		assertEquals(Long.valueOf(2L), request.getExpectedVersion());

		assertThrows(RuleValidationException.class, () -> PublishChainRequest.builder()
				.chainId("c1").el("THEN(a)").expectedVersion(-1L).build());
	}

	@Test
	void scriptRequestExposesOptionalFieldsAndEnumTypeOverload() {
		PublishScriptRequest request = PublishScriptRequest.builder().nodeId("s1")
				.script("return true").type(NodeTypeEnum.SCRIPT).name("named").language("groovy")
				.expectedVersion(4L).build();

		assertEquals("s1", request.getNodeId());
		assertEquals("s1", request.getTargetId());
		assertEquals("return true", request.getScript());
		assertEquals("script", request.getType());
		assertEquals("named", request.getName());
		assertEquals("groovy", request.getLanguage());
		assertEquals(Long.valueOf(4L), request.getExpectedVersion());

		assertThrows(RuleValidationException.class, () -> PublishScriptRequest.builder()
				.nodeId("s1").script("return true").type((NodeTypeEnum) null).build());
		assertThrows(RuleValidationException.class, () -> PublishScriptRequest.builder()
				.nodeId("s1").script("return true").type("not-a-type").build());
		assertThrows(RuleValidationException.class, () -> PublishScriptRequest.builder()
				.nodeId(" ").script("return true").type(NodeTypeEnum.SCRIPT).build());
		assertThrows(RuleValidationException.class, () -> PublishScriptRequest.builder()
				.nodeId("s1").script(" ").type(NodeTypeEnum.SCRIPT).build());
		assertThrows(RuleValidationException.class, () -> PublishScriptRequest.builder()
				.nodeId("s1").script("return true").type(NodeTypeEnum.SCRIPT)
				.expectedVersion(-1L).build());
	}

	@Test
	void removeRequestRejectsBlankAndMissingTargetId() {
		assertThrows(RuleValidationException.class,
				() -> RemoveRuleRequest.builder().targetId(" ").build());
		assertThrows(RuleValidationException.class,
				() -> RemoveRuleRequest.builder().targetId(null).build());
		assertThrows(RuleValidationException.class,
				() -> RemoveRuleRequest.builder().build());

		RemoveRuleRequest request = RemoveRuleRequest.builder().targetId("c1").build();
		assertEquals("c1", request.getTargetId());
		assertNull(request.getExpectedVersion());
	}

	@Test
	void exceptionsCarryMessageAndCause() {
		RuntimeException cause = new RuntimeException("root");

		PublisherConfigurationException configuration =
				new PublisherConfigurationException("config", cause);
		assertEquals("config", configuration.getMessage());
		assertSame(cause, configuration.getCause());

		RuleStorageException storage = new RuleStorageException("storage", cause);
		assertEquals("storage", storage.getMessage());
		assertSame(cause, storage.getCause());

		RuleValidationException validation = new RuleValidationException("validation", cause);
		assertEquals("validation", validation.getMessage());
		assertSame(cause, validation.getCause());

		VersionConflictException conflict = new VersionConflictException("conflict", cause);
		assertEquals("conflict", conflict.getMessage());
		assertSame(cause, conflict.getCause());
	}

	private static RulePublisherConfig config(String applicationName, PublisherBackend backend) {
		return new RulePublisherConfig() {
			@Override
			public String applicationName() {
				return applicationName;
			}

			@Override
			public PublisherBackend backend() {
				return backend;
			}
		};
	}

	private static class NoOpPublisher implements RulePublisher {

		@Override
		public PublishResult publishChain(PublishChainRequest request) {
			return null;
		}

		@Override
		public PublishResult publishScript(PublishScriptRequest request) {
			return null;
		}

		@Override
		public PublishResult removeChain(RemoveRuleRequest request) {
			return null;
		}

		@Override
		public PublishResult removeScript(RemoveRuleRequest request) {
			return null;
		}

		@Override
		public void close() {
		}
	}

	private static final class ResultProvider implements RulePublisherProvider {
		@Override public boolean supports(RulePublisherConfig config) { return true; }
		@Override public RulePublisher create(RulePublisherConfig config) {
			return new NoOpPublisher() {
				@Override public PublishResult removeChain(RemoveRuleRequest request) {
					return PublishResult.builder().targetId("another")
							.targetType(ChangeRecord.TargetType.CHAIN).operation(ChangeRecord.Op.DELETE)
							.version(1).sequence(1).build();
				}
			};
		}
	}
}
