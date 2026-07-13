package com.yomahub.liteflow.repository.zk;

import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.property.RuleDbZkConfig;
import com.yomahub.liteflow.repository.RuleChangeSource;
import com.yomahub.liteflow.repository.RuleDbProvider;
import com.yomahub.liteflow.repository.RuleRepository;

public final class ZkRuleDbProvider implements RuleDbProvider {

	private final ZkConnectionManager connection;
	private final ZkRuleRepository repository;
	private final ZkCacheChangeSource changeSource;

	public ZkRuleDbProvider() {
		RuleDbConfig config = LiteflowConfigGetter.get().getRuleDb();
		RuleDbZkConfig zk = config == null || config.getZk() == null ? new RuleDbZkConfig() : config.getZk();
		String applicationName = config == null ? null : config.getApplicationName();
		if (StrUtil.isBlank(applicationName)) { applicationName = "default"; }
		this.connection = new ZkConnectionManager(zk);
		ZkPaths paths = new ZkPaths(zk.getRootPath(), applicationName);
		ZkRecordCodec codec = new ZkRecordCodec();
		this.repository = new ZkRuleRepository(connection.client(), paths, codec);
		this.changeSource = new ZkCacheChangeSource(connection.client(), paths, codec);
	}

	@Override public String type() { return "zk"; }
	@Override public RuleRepository repository() { return repository; }
	@Override public RuleChangeSource changeSource() { return changeSource; }

	@Override
	public void close() {
		changeSource.close();
		connection.close();
	}
}
