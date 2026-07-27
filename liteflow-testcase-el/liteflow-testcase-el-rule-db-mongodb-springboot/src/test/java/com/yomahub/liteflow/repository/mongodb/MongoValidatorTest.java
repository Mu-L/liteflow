package com.yomahub.liteflow.repository.mongodb;

import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.PublishScriptRequest;
import com.yomahub.liteflow.publisher.PublisherBackend;
import com.yomahub.liteflow.publisher.RemoveRuleRequest;
import com.yomahub.liteflow.publisher.RulePublisherConfig;
import com.yomahub.liteflow.publisher.exception.RuleValidationException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/** Validation, naming, and config-object branch coverage. */
class MongoValidatorTest {

	@Test
	void storageValidatorDefaultsAndLimits() {
		assertEquals("default", MongoStorageValidator.applicationNameOrDefault(null));
		assertEquals("default", MongoStorageValidator.applicationNameOrDefault("  "));
		assertThrows(ConfigErrorException.class,
				() -> MongoStorageValidator.applicationNameOrDefault(repeat("a", 129)));

		assertEquals("liteflow", MongoStorageValidator.databaseOrDefault(null));
		assertThrows(ConfigErrorException.class, () -> MongoStorageValidator.databaseOrDefault("bad.name"));

		assertEquals("lf_", MongoStorageValidator.collectionPrefixOrDefault(null));
		assertThrows(ConfigErrorException.class, () -> MongoStorageValidator.collectionPrefixOrDefault("lf-$"));
		assertThrows(ConfigErrorException.class,
				() -> MongoStorageValidator.collectionPrefixOrDefault(repeat("p", 65)));

		MongoStorageValidator.validateTargetId("chainId", "ok");
		assertThrows(ConfigErrorException.class, () -> MongoStorageValidator.validateTargetId("chainId", " "));
		assertThrows(ConfigErrorException.class,
				() -> MongoStorageValidator.validateTargetId("chainId", repeat("c", 129)));
	}

	@Test
	void requestValidationRejectsOversizedFields() {
		MongoStorageValidator.validateChainRequest(PublishChainRequest.builder()
				.chainId("c1").el("THEN(a)").build());
		assertThrows(RuleValidationException.class, () -> MongoStorageValidator.validateChainRequest(
				PublishChainRequest.builder().chainId(repeat("c", 129)).el("THEN(a)").build()));
		assertThrows(RuleValidationException.class, () -> MongoStorageValidator.validateChainRequest(
				PublishChainRequest.builder().chainId("c1").el("THEN(a)").namespace(repeat("n", 129)).build()));

		MongoStorageValidator.validateScriptRequest(PublishScriptRequest.builder()
				.nodeId("s1").script("x").type("script").build());
		assertThrows(RuleValidationException.class, () -> MongoStorageValidator.validateScriptRequest(
				PublishScriptRequest.builder().nodeId(repeat("n", 129)).script("x").type("script").build()));
		assertThrows(RuleValidationException.class, () -> MongoStorageValidator.validateScriptRequest(
				PublishScriptRequest.builder().nodeId("s1").script("x").type("script")
						.name(repeat("n", 257)).build()));
		assertThrows(RuleValidationException.class, () -> MongoStorageValidator.validateScriptRequest(
				PublishScriptRequest.builder().nodeId("s1").script("x").type("script")
						.language(repeat("l", 65)).build()));

		MongoStorageValidator.validateRemoveRequest(RemoveRuleRequest.builder().targetId("c1").build());
		assertThrows(RuleValidationException.class, () -> MongoStorageValidator.validateRemoveRequest(
				RemoveRuleRequest.builder().targetId(repeat("t", 129)).build()));
	}

	@Test
	void publisherConfigBuilderExposesDefaultsAndBackend() {
		MongoPublisherConfig config = MongoPublisherConfig.builder().applicationName("app").build();
		assertEquals("app", config.applicationName());
		assertEquals(PublisherBackend.MONGODB, config.backend());
		assertEquals("liteflow", config.getDatabase());
		assertEquals("lf_", config.getCollectionPrefix());
		assertEquals(null, config.getUri());
		assertEquals(null, config.getMongoClient());
	}

	@Test
	void collectionsApplyValidatedPrefix() {
		MongoCollections names = new MongoCollections("custom_");
		assertEquals("custom_chain", names.chains());
		assertEquals("custom_script", names.scripts());
		assertEquals("custom_change_log", names.changes());
		assertEquals("custom_sequence", names.sequences());
		assertThrows(ConfigErrorException.class, () -> new MongoCollections("bad-prefix"));
	}

	@Test
	void publisherProviderSupportsOnlyMongoPublisherConfig() {
		MongoRulePublisherProvider provider = new MongoRulePublisherProvider();
		assertFalse(provider.supports(mock(RulePublisherConfig.class)));
		assertTrue(provider.supports(MongoPublisherConfig.builder().applicationName("app").build()));
	}

	private String repeat(String value, int count) {
		StringBuilder result = new StringBuilder(value.length() * count);
		for (int i = 0; i < count; i++) {
			result.append(value);
		}
		return result.toString();
	}
}
