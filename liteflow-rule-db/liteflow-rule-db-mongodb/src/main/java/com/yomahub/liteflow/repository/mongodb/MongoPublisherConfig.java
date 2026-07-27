package com.yomahub.liteflow.repository.mongodb;

import com.mongodb.client.MongoClient;
import com.yomahub.liteflow.publisher.PublisherBackend;
import com.yomahub.liteflow.publisher.RulePublisherConfig;

/** Type-safe connection configuration for an independent MongoDB publisher. */
public final class MongoPublisherConfig implements RulePublisherConfig {

	private final String applicationName;
	private final String uri;
	private final String database;
	private final String collectionPrefix;
	private final MongoClient mongoClient;

	private MongoPublisherConfig(Builder builder) {
		this.applicationName = builder.applicationName;
		this.uri = builder.uri;
		this.database = builder.database;
		this.collectionPrefix = builder.collectionPrefix;
		this.mongoClient = builder.mongoClient;
	}

	public static Builder builder() {
		return new Builder();
	}

	@Override
	public String applicationName() {
		return applicationName;
	}

	@Override
	public PublisherBackend backend() {
		return PublisherBackend.MONGODB;
	}

	public String getUri() {
		return uri;
	}

	public String getDatabase() {
		return database;
	}

	public String getCollectionPrefix() {
		return collectionPrefix;
	}

	public MongoClient getMongoClient() {
		return mongoClient;
	}

	public static final class Builder {

		private String applicationName;
		private String uri;
		private String database = "liteflow";
		private String collectionPrefix = "lf_";
		private MongoClient mongoClient;

		private Builder() {
		}

		public Builder applicationName(String applicationName) {
			this.applicationName = applicationName;
			return this;
		}

		public Builder uri(String uri) {
			this.uri = uri;
			return this;
		}

		public Builder database(String database) {
			this.database = database;
			return this;
		}

		public Builder collectionPrefix(String collectionPrefix) {
			this.collectionPrefix = collectionPrefix;
			return this;
		}

		public Builder mongoClient(MongoClient mongoClient) {
			this.mongoClient = mongoClient;
			return this;
		}

		public MongoPublisherConfig build() {
			return new MongoPublisherConfig(this);
		}
	}
}
