package com.yomahub.liteflow.repository.zk;

import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.publisher.PublisherBackend;
import com.yomahub.liteflow.publisher.RulePublisher;
import com.yomahub.liteflow.publisher.RulePublisherConfig;
import com.yomahub.liteflow.publisher.RulePublisherProvider;
import com.yomahub.liteflow.publisher.exception.PublisherConfigurationException;

public final class ZkRulePublisherProvider implements RulePublisherProvider {

	@Override
	public boolean supports(RulePublisherConfig config) {
		return config instanceof ZkPublisherConfig && config.backend() == PublisherBackend.ZK;
	}

	@Override
	public RulePublisher create(RulePublisherConfig config) {
		if (!(config instanceof ZkPublisherConfig)) {
			throw new PublisherConfigurationException("ZooKeeper publisher requires ZkPublisherConfig");
		}
		ZkPublisherConfig zk = (ZkPublisherConfig) config;
		if (StrUtil.isBlank(zk.applicationName())) {
			throw new PublisherConfigurationException("ZooKeeper publisher applicationName must not be blank");
		}
		if (zk.getClient() == null && StrUtil.isBlank(zk.getConnectString())) {
			throw new PublisherConfigurationException("ZooKeeper publisher requires a CuratorFramework or connectString");
		}
		if (StrUtil.isBlank(zk.getRootPath())) {
			throw new PublisherConfigurationException("ZooKeeper publisher rootPath must not be blank");
		}
		return new ZkRulePublisher(zk);
	}
}
