package com.yomahub.liteflow.repository.etcd;

import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.publisher.PublisherBackend;
import com.yomahub.liteflow.publisher.RulePublisher;
import com.yomahub.liteflow.publisher.RulePublisherConfig;
import com.yomahub.liteflow.publisher.RulePublisherProvider;
import com.yomahub.liteflow.publisher.exception.PublisherConfigurationException;

public final class EtcdRulePublisherProvider implements RulePublisherProvider {

	@Override
	public boolean supports(RulePublisherConfig config) {
		return config instanceof EtcdPublisherConfig && config.backend() == PublisherBackend.ETCD;
	}

	@Override
	public RulePublisher create(RulePublisherConfig config) {
		if (!(config instanceof EtcdPublisherConfig)) {
			throw new PublisherConfigurationException("etcd publisher requires EtcdPublisherConfig");
		}
		EtcdPublisherConfig etcd = (EtcdPublisherConfig) config;
		if (etcd.getClient() == null && StrUtil.isBlank(etcd.getEndpoints())) {
			throw new PublisherConfigurationException("etcd publisher requires a Client or endpoints");
		}
		if (StrUtil.isBlank(etcd.applicationName())) {
			throw new PublisherConfigurationException("etcd publisher applicationName must not be blank");
		}
		if (StrUtil.isBlank(etcd.getRootPath())) {
			throw new PublisherConfigurationException("etcd publisher rootPath must not be blank");
		}
		return new EtcdRulePublisher(etcd);
	}
}
