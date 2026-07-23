package com.yomahub.liteflow.repository.mongodb;

import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.property.RuleDbMongoConfig;
import com.yomahub.liteflow.repository.RuleChangeSource;
import com.yomahub.liteflow.repository.RuleDbProvider;
import com.yomahub.liteflow.repository.RuleRepository;

/** MongoDB execution provider. */
public final class MongoRuleDbProvider implements RuleDbProvider {

	private final MongoConnectionManager connection;
	private final MongoRuleRepository repository;
	private final MongoPollingChangeSource changeSource;

	public MongoRuleDbProvider() {
		RuleDbConfig config = LiteflowConfigGetter.get().getRuleDb();
		RuleDbMongoConfig mongodb = config == null || config.getMongodb() == null
				? new RuleDbMongoConfig() : config.getMongodb();
		String applicationName = MongoStorageValidator.applicationNameOrDefault(
				config == null ? null : config.getApplicationName());
		this.connection = new MongoConnectionManager(mongodb);
		MongoCollections names = new MongoCollections(mongodb.getCollectionPrefix());
		MongoSchema.ensureIndexes(connection.database(), names);
		this.repository = new MongoRuleRepository(connection.client(), connection.database(), names, applicationName);
		int pollSeconds = config == null || config.getSync() == null || config.getSync().getPollSeconds() == null
				? 3 : config.getSync().getPollSeconds();
		int batchSize = mongodb.getChangeLogBatchSize() == null ? 1000 : mongodb.getChangeLogBatchSize();
		this.changeSource = new MongoPollingChangeSource(repository, pollSeconds, batchSize);
	}

	@Override public String type() { return "mongodb"; }
	@Override public RuleRepository repository() { return repository; }
	@Override public RuleChangeSource changeSource() { return changeSource; }
	@Override public void close() { changeSource.close(); connection.close(); }
}
