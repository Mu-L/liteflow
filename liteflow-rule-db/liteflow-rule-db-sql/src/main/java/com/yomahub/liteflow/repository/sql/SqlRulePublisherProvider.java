package com.yomahub.liteflow.repository.sql;

import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.publisher.PublisherBackend;
import com.yomahub.liteflow.publisher.RulePublisher;
import com.yomahub.liteflow.publisher.RulePublisherConfig;
import com.yomahub.liteflow.publisher.RulePublisherProvider;
import com.yomahub.liteflow.publisher.exception.PublisherConfigurationException;

/** Service provider for SQL-backed rule publishers. */
public class SqlRulePublisherProvider implements RulePublisherProvider {

	@Override
	public boolean supports(RulePublisherConfig config) {
		return config instanceof SqlPublisherConfig && config.backend() == PublisherBackend.SQL;
	}

	@Override
	public RulePublisher create(RulePublisherConfig config) {
		if (!(config instanceof SqlPublisherConfig)) {
			throw new PublisherConfigurationException("SQL publisher requires SqlPublisherConfig");
		}
		SqlPublisherConfig sql = (SqlPublisherConfig) config;
		if (sql.getDataSource() == null && StrUtil.isBlank(sql.getUrl())) {
			throw new PublisherConfigurationException("SQL publisher requires a DataSource or JDBC url");
		}
		try {
			SqlStorageValidator.validateApplicationName(sql.applicationName());
			SqlStorageValidator.validateTablePrefix(sql.getTablePrefix());
		}
		catch (ConfigErrorException e) {
			throw new PublisherConfigurationException(e.getMessage(), e);
		}
		return new SqlRulePublisherImpl(sql);
	}
}
