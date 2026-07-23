package com.yomahub.liteflow.repository.mongodb;

import com.mongodb.MongoException;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.yomahub.liteflow.publisher.exception.RuleStorageException;
import static com.mongodb.client.model.Indexes.ascending;
import static com.mongodb.client.model.Indexes.compoundIndex;

/** Creates the indexes required by manifest and polling queries. */
final class MongoSchema {

	private MongoSchema() { }

	static void ensureIndexes(MongoDatabase database, MongoCollections names) {
		try {
			database.getCollection(names.chains()).createIndex(ascending("applicationName"));
			database.getCollection(names.scripts()).createIndex(ascending("applicationName"));
			database.getCollection(names.changes()).createIndex(
					compoundIndex(ascending("applicationName"), ascending("seq")), new IndexOptions().unique(true));
		}
		catch (MongoException e) {
			throw new RuleStorageException("MongoDB failed to ensure rule-db indexes: " + e.getMessage(), e);
		}
	}
}
