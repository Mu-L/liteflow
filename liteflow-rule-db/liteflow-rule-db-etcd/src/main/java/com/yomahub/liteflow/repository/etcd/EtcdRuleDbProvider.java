package com.yomahub.liteflow.repository.etcd;

import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.property.RuleDbEtcdConfig;
import com.yomahub.liteflow.repository.RuleChangeSource;
import com.yomahub.liteflow.repository.RuleDbProvider;
import com.yomahub.liteflow.repository.RuleRepository;

public final class EtcdRuleDbProvider implements RuleDbProvider {

	private final EtcdConnectionManager connection;
	private final EtcdRuleRepository repository;
	private final EtcdWatchChangeSource changeSource;

	public EtcdRuleDbProvider() {
		RuleDbConfig config = LiteflowConfigGetter.get().getRuleDb();
		RuleDbEtcdConfig etcd = config == null || config.getEtcd() == null
				? new RuleDbEtcdConfig() : config.getEtcd();
		String applicationName = config == null ? null : config.getApplicationName();
		if (StrUtil.isBlank(applicationName)) {
			applicationName = "default";
		}
		this.connection = new EtcdConnectionManager(etcd);
		EtcdKeys keys = new EtcdKeys(etcd.getRootPath(), applicationName);
		EtcdRecordCodec codec = new EtcdRecordCodec();
		this.repository = new EtcdRuleRepository(
				new JetcdKvFacade(connection.client().getKVClient()), keys, codec);
		this.changeSource = new EtcdWatchChangeSource(
				new JetcdWatchFacade(connection.client().getWatchClient()), keys, codec);
	}

	@Override
	public String type() {
		return "etcd";
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
		connection.close();
	}
}
