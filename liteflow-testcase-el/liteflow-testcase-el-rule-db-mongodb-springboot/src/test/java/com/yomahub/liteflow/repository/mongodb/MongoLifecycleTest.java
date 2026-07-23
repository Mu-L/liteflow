package com.yomahub.liteflow.repository.mongodb;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.property.LiteflowConfig;
import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.property.RuleDbMongoConfig;
import com.yomahub.liteflow.publisher.exception.RuleStorageException;
import com.yomahub.liteflow.spi.ContextAware;
import com.yomahub.liteflow.spi.holder.ContextAwareHolder;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Connection ownership, lifecycle, and construction-failure semantics. */
@SuppressWarnings({ "unchecked", "rawtypes" })
class MongoLifecycleTest {

	private static final String UNREACHABLE_URI = "mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=300";

	@AfterEach
	void cleanConfiguration() {
		LiteflowConfigGetter.clean();
		ContextAwareHolder.clean();
	}

	@Test
	void publisherConnectionManagerRejectsMissingClientAndUri() {
		MongoPublisherConfig config = MongoPublisherConfig.builder().applicationName("app").build();
		ConfigErrorException error = assertThrows(ConfigErrorException.class,
				() -> new MongoConnectionManager(config));
		assertTrue(error.getMessage().contains("requires a MongoClient or uri"));
	}

	@Test
	void executionConnectionManagerRejectsMissingBeanAndUri() {
		ConfigErrorException error = assertThrows(ConfigErrorException.class,
				() -> new MongoConnectionManager(new RuleDbMongoConfig()));
		assertTrue(error.getMessage().contains("requires a MongoClient bean"));
	}

	@Test
	void borrowedClientIsNotClosedByConnectionManager() {
		MongoClient client = mock(MongoClient.class);
		MongoDatabase database = mock(MongoDatabase.class);
		when(client.getDatabase("rules")).thenReturn(database);
		MongoPublisherConfig config = MongoPublisherConfig.builder().applicationName("app")
				.database("rules").mongoClient(client).build();

		MongoConnectionManager manager = new MongoConnectionManager(config);
		manager.close();

		verify(client, never()).close();
	}

	@Test
	void ownedClientIsClosedByConnectionManager() {
		MongoPublisherConfig config = MongoPublisherConfig.builder().applicationName("app")
				.uri("mongodb://127.0.0.1:27017").build();
		MongoConnectionManager manager = new MongoConnectionManager(config);
		MongoClient owned = manager.client();

		manager.close();

		assertThrows(IllegalStateException.class,
				() -> owned.getDatabase("probe").listCollectionNames().first());
	}

	@Test
	void publisherConstructionFailureWrapsDriverErrorAndKeepsBorrowedClientOpen() {
		MongoClient client = mock(MongoClient.class);
		MongoDatabase database = mock(MongoDatabase.class);
		MongoCollection<Document> collection = mock(MongoCollection.class);
		when(client.getDatabase("rules")).thenReturn(database);
		when(database.getCollection(anyString())).thenReturn(collection);
		when(collection.createIndex(any(Bson.class))).thenThrow(new MongoException(13, "not authorized"));
		when(collection.createIndex(any(Bson.class), any())).thenThrow(new MongoException(13, "not authorized"));
		MongoPublisherConfig config = MongoPublisherConfig.builder().applicationName("app")
				.database("rules").mongoClient(client).build();

		RuleStorageException error = assertThrows(RuleStorageException.class,
				() -> new MongoRulePublisherProvider().create(config));

		assertTrue(error.getMessage().contains("ensure"));
		verify(client, never()).close();
	}

	@Test
	void publisherConstructionFailureReleasesOwnedClient() {
		MongoPublisherConfig config = MongoPublisherConfig.builder().applicationName("app")
				.uri(UNREACHABLE_URI).build();

		RuleStorageException error = assertThrows(RuleStorageException.class,
				() -> new MongoRulePublisherProvider().create(config));
		assertTrue(error.getMessage().contains("ensure"));
	}

	@Test
	void providerConstructionWithoutMongoConfigThrows() {
		LiteflowConfigGetter.setLiteflowConfig(new LiteflowConfig());
		try {
			ConfigErrorException error = assertThrows(ConfigErrorException.class, MongoRuleDbProvider::new);
			assertTrue(error.getMessage().contains("requires a MongoClient bean"));
		}
		finally {
			LiteflowConfigGetter.clean();
		}
	}

	@Test
	void executionProviderDoesNotPerformSchemaWritesAtStartup() throws Exception {
		MongoClient client = mock(MongoClient.class);
		MongoDatabase database = mock(MongoDatabase.class);
		when(client.getDatabase("liteflow")).thenReturn(database);
		ContextAware contextAware = mock(ContextAware.class);
		when(contextAware.getBean("readOnlyMongo")).thenReturn(client);
		installContextAware(contextAware);

		LiteflowConfig liteflowConfig = new LiteflowConfig();
		RuleDbConfig ruleDb = new RuleDbConfig();
		RuleDbMongoConfig mongodb = new RuleDbMongoConfig();
		mongodb.setMongoClientBeanName("readOnlyMongo");
		ruleDb.setMongodb(mongodb);
		liteflowConfig.setRuleDb(ruleDb);
		LiteflowConfigGetter.setLiteflowConfig(liteflowConfig);

		MongoRuleDbProvider provider = new MongoRuleDbProvider();
		provider.close();

		verify(database, never()).getCollection(anyString());
		verify(client, never()).close();
	}

	@Test
	void executionProviderWithOwnedClientDefersNetworkIoUntilRepositoryUse() {
		LiteflowConfig liteflowConfig = new LiteflowConfig();
		RuleDbConfig ruleDb = new RuleDbConfig();
		RuleDbMongoConfig mongodb = new RuleDbMongoConfig();
		mongodb.setUri(UNREACHABLE_URI);
		ruleDb.setMongodb(mongodb);
		liteflowConfig.setRuleDb(ruleDb);
		LiteflowConfigGetter.setLiteflowConfig(liteflowConfig);

		MongoRuleDbProvider provider = new MongoRuleDbProvider();
		provider.close();
	}

	private static void installContextAware(ContextAware contextAware) throws Exception {
		Field field = ContextAwareHolder.class.getDeclaredField("contextAware");
		field.setAccessible(true);
		field.set(null, contextAware);
	}
}
