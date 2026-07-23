package com.yomahub.liteflow.repository.postgresql;

import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.publisher.PublisherBackend;
import com.yomahub.liteflow.publisher.RulePublisher;
import com.yomahub.liteflow.publisher.RulePublisherConfig;
import com.yomahub.liteflow.publisher.RulePublisherProvider;
import com.yomahub.liteflow.publisher.exception.PublisherConfigurationException;

/** Service provider for PostgreSQL-backed publishers. */
public final class PostgresqlRulePublisherProvider implements RulePublisherProvider {

	@Override
	public boolean supports(RulePublisherConfig config) {
		return config instanceof PostgresqlPublisherConfig && config.backend() == PublisherBackend.POSTGRESQL;
	}

	@Override
	public RulePublisher create(RulePublisherConfig config) {
		if (!(config instanceof PostgresqlPublisherConfig)) {
			throw new PublisherConfigurationException("PostgreSQL publisher requires PostgresqlPublisherConfig");
		}
		PostgresqlPublisherConfig postgresql = (PostgresqlPublisherConfig) config;
		if (postgresql.getDataSource() == null && StrUtil.isBlank(postgresql.getUrl())) {
			throw new PublisherConfigurationException("PostgreSQL publisher requires a DataSource or JDBC url");
		}
		try {
			PostgresqlStorageValidator.validateApplicationName(postgresql.applicationName());
			PostgresqlStorageValidator.validateTablePrefix(postgresql.getTablePrefix());
		}
		catch (ConfigErrorException e) { throw new PublisherConfigurationException(e.getMessage(), e); }
		return new PostgresqlRulePublisher(postgresql);
	}
}
