package com.yomahub.liteflow.repository.sql;

import cn.hutool.core.util.StrUtil;
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
		SqlConnectionManager connectionManager = new SqlConnectionManager(sql);
		SqlDialect dialect = new SqlDialect(sql.getTablePrefix());
		String applicationName = config == null ? null : config.getApplicationName();
		if (StrUtil.isBlank(applicationName)) {
			applicationName = "default";
		}
		boolean autoInitTable = Boolean.TRUE.equals(sql.getAutoInitTable());
		this.repository = new SqlRuleRepository(connectionManager, dialect, applicationName, autoInitTable);
		int pollSeconds = config == null || config.getSync() == null
				|| config.getSync().getPollSeconds() == null ? 3 : config.getSync().getPollSeconds();
		this.changeSource = new SqlPollingChangeSource(repository, pollSeconds);
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
