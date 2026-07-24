package com.yomahub.liteflow.repository.nacos;

import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.publisher.PublisherBackend;
import com.yomahub.liteflow.publisher.RulePublisher;
import com.yomahub.liteflow.publisher.RulePublisherConfig;
import com.yomahub.liteflow.publisher.RulePublisherProvider;
import com.yomahub.liteflow.publisher.exception.PublisherConfigurationException;

/** Service provider for Nacos-backed rule publishers. */
public final class NacosRulePublisherProvider implements RulePublisherProvider {

	@Override
	public boolean supports(RulePublisherConfig config) {
		return config instanceof NacosPublisherConfig && config.backend() == PublisherBackend.NACOS;
	}

	@Override
	public RulePublisher create(RulePublisherConfig config) {
		if (!(config instanceof NacosPublisherConfig)) {
			throw new PublisherConfigurationException("Nacos publisher requires NacosPublisherConfig");
		}
		NacosPublisherConfig nacos = (NacosPublisherConfig) config;
		if (StrUtil.isBlank(nacos.applicationName())) {
			throw new PublisherConfigurationException("Nacos publisher applicationName must not be blank");
		}
		if (nacos.getConfigService() == null && StrUtil.isBlank(nacos.getServerAddr())) {
			throw new PublisherConfigurationException("Nacos publisher requires a ConfigService or serverAddr");
		}
		if (nacos.getTimeoutMillis() == null || nacos.getTimeoutMillis() <= 0) {
			throw new PublisherConfigurationException("Nacos publisher timeoutMillis must be greater than zero");
		}
		try {
			new NacosConfigKey(nacos.getDataIdPrefix(), nacos.applicationName(), nacos.getGroup());
			return new NacosRulePublisher(nacos);
		}
		catch (ConfigErrorException e) {
			throw new PublisherConfigurationException(e.getMessage(), e);
		}
	}
}
