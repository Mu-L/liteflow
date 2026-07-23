package com.yomahub.liteflow.repository.postgresql;

import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.property.RuleDbPostgresqlConfig;
import com.yomahub.liteflow.repository.RuleChangeSource;
import com.yomahub.liteflow.repository.RuleDbProvider;
import com.yomahub.liteflow.repository.RuleRepository;

/** PostgreSQL execution provider. */
public final class PostgresqlRuleDbProvider implements RuleDbProvider {

	private final PostgresqlRuleRepository repository;
	private final PostgresqlPollingChangeSource changeSource;

	public PostgresqlRuleDbProvider() {
		RuleDbConfig config = LiteflowConfigGetter.get().getRuleDb();
		RuleDbPostgresqlConfig postgresql = config == null || config.getPostgresql() == null
				? new RuleDbPostgresqlConfig() : config.getPostgresql();
		PostgresqlStorageValidator.validateTablePrefix(postgresql.getTablePrefix());
		String applicationName = PostgresqlStorageValidator.applicationNameOrDefault(
				config == null ? null : config.getApplicationName());
		this.repository = new PostgresqlRuleRepository(new PostgresqlConnectionManager(postgresql),
				new PostgresqlDialect(postgresql.getTablePrefix()), applicationName,
				Boolean.TRUE.equals(postgresql.getAutoInitTable()));
		int pollSeconds = config == null || config.getSync() == null || config.getSync().getPollSeconds() == null
				? 3 : config.getSync().getPollSeconds();
		int batchSize = postgresql.getChangeLogBatchSize() == null ? 1000 : postgresql.getChangeLogBatchSize();
		this.changeSource = new PostgresqlPollingChangeSource(repository, pollSeconds, batchSize);
	}

	@Override public String type() { return "postgresql"; }
	@Override public RuleRepository repository() { return repository; }
	@Override public RuleChangeSource changeSource() { return changeSource; }
	@Override public void close() { changeSource.close(); }
}
