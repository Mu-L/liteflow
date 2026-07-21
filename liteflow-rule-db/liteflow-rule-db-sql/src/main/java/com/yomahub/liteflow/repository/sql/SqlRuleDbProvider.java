package com.yomahub.liteflow.repository.sql;

import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.property.RuleDbSqlConfig;
import com.yomahub.liteflow.repository.RuleChangeSource;
import com.yomahub.liteflow.repository.RuleDbProvider;
import com.yomahub.liteflow.repository.RuleRepository;

/** SQL execution provider sharing one connection configuration across runtime adapters. */
public class SqlRuleDbProvider implements RuleDbProvider {

	private final SqlRuleRepository repository;
	private final SqlPollingChangeSource changeSource;

	public SqlRuleDbProvider() {
		RuleDbConfig config = LiteflowConfigGetter.get().getRuleDb();
		RuleDbSqlConfig sql = config == null || config.getSql() == null
				? new RuleDbSqlConfig() : config.getSql();
		SqlStorageValidator.validateTablePrefix(sql.getTablePrefix());
		SqlConnectionManager connectionManager = new SqlConnectionManager(sql);
		SqlDialect dialect = new SqlDialect(sql.getTablePrefix());
		String applicationName = config == null ? null : config.getApplicationName();
		applicationName = SqlStorageValidator.applicationNameOrDefault(applicationName);
		boolean autoInitTable = Boolean.TRUE.equals(sql.getAutoInitTable());
		this.repository = new SqlRuleRepository(connectionManager, dialect, applicationName, autoInitTable);
		int pollSeconds = config == null || config.getSync() == null
				|| config.getSync().getPollSeconds() == null ? 3 : config.getSync().getPollSeconds();
		int batchSize = sql.getChangeLogBatchSize() == null ? 1000 : sql.getChangeLogBatchSize();
		this.changeSource = new SqlPollingChangeSource(repository, pollSeconds, batchSize);
	}

	@Override
	public String type() {
		return "sql";
	}

	@Override
	public RuleRepository repository() {
		return repository;
	}

	@Override
	public RuleChangeSource changeSource() {
		return changeSource;
	}

	@Override
	public void close() {
		changeSource.close();
	}
}
