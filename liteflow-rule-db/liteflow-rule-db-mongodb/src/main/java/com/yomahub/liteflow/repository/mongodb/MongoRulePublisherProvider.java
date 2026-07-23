package com.yomahub.liteflow.repository.mongodb;

import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.publisher.PublisherBackend;
import com.yomahub.liteflow.publisher.RulePublisher;
import com.yomahub.liteflow.publisher.RulePublisherConfig;
import com.yomahub.liteflow.publisher.RulePublisherProvider;
import com.yomahub.liteflow.publisher.exception.PublisherConfigurationException;

/** Service provider for MongoDB-backed publishers. */
public final class MongoRulePublisherProvider implements RulePublisherProvider {

	@Override
	public boolean supports(RulePublisherConfig config) {
		return config instanceof MongoPublisherConfig && config.backend() == PublisherBackend.MONGODB;
	}

	@Override
	public RulePublisher create(RulePublisherConfig config) {
		if (!(config instanceof MongoPublisherConfig)) {
			throw new PublisherConfigurationException("MongoDB publisher requires MongoPublisherConfig");
		}
		MongoPublisherConfig mongo = (MongoPublisherConfig) config;
		if (mongo.getMongoClient() == null && StrUtil.isBlank(mongo.getUri())) {
			throw new PublisherConfigurationException("MongoDB publisher requires a MongoClient or uri");
		}
		try {
			MongoStorageValidator.applicationNameOrDefault(mongo.applicationName());
			MongoStorageValidator.databaseOrDefault(mongo.getDatabase());
			MongoStorageValidator.collectionPrefixOrDefault(mongo.getCollectionPrefix());
		}
		catch (ConfigErrorException e) { throw new PublisherConfigurationException(e.getMessage(), e); }
		return new MongoRulePublisher(mongo);
	}
}
