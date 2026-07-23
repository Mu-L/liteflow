package com.yomahub.liteflow.repository.mongodb;

import cn.hutool.core.util.StrUtil;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.property.RuleDbMongoConfig;
import com.yomahub.liteflow.spi.holder.ContextAwareHolder;

/** Owns or borrows one MongoClient and resolves the configured database. */
final class MongoConnectionManager implements AutoCloseable {

	private final MongoClient client;
	private final MongoDatabase database;
	private final boolean owned;

	MongoConnectionManager(RuleDbMongoConfig config) {
		MongoClient borrowed = lookup(config.getMongoClientBeanName());
		if (borrowed != null) {
			this.client = borrowed;
			this.owned = false;
		}
		else {
			if (StrUtil.isBlank(config.getUri())) {
				throw new ConfigErrorException("rule-db mongodb requires a MongoClient bean or liteflow.rule-db.mongodb.uri");
			}
			this.client = MongoClients.create(config.getUri());
			this.owned = true;
		}
		this.database = client.getDatabase(MongoStorageValidator.databaseOrDefault(config.getDatabase()));
	}

	MongoConnectionManager(MongoPublisherConfig config) {
		if (config.getMongoClient() != null) {
			this.client = config.getMongoClient();
			this.owned = false;
		}
		else {
			if (StrUtil.isBlank(config.getUri())) {
				throw new ConfigErrorException("MongoDB publisher requires a MongoClient or uri");
			}
			this.client = MongoClients.create(config.getUri());
			this.owned = true;
		}
		this.database = client.getDatabase(MongoStorageValidator.databaseOrDefault(config.getDatabase()));
	}

	MongoClient client() { return client; }
	MongoDatabase database() { return database; }

	@Override public void close() { if (owned) { client.close(); } }

	private MongoClient lookup(String beanName) {
		try {
			if (StrUtil.isNotBlank(beanName)) { return ContextAwareHolder.loadContextAware().getBean(beanName); }
			return ContextAwareHolder.loadContextAware().getBean(MongoClient.class);
		}
		catch (Exception ignored) { return null; }
	}
}
