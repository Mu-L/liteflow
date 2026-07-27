package com.yomahub.liteflow.repository.nacos;

import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.property.RuleDbNacosConfig;
import com.yomahub.liteflow.repository.RuleChangeSource;
import com.yomahub.liteflow.repository.RuleDbProvider;
import com.yomahub.liteflow.repository.RuleRepository;

/** Execution-side Nacos Rule-DB provider. */
public final class NacosRuleDbProvider implements RuleDbProvider {

	private final NacosConnectionManager connection;
	private final NacosRuleRepository repository;
	private final NacosListenerChangeSource changeSource;

	public NacosRuleDbProvider() {
		RuleDbConfig config = LiteflowConfigGetter.get().getRuleDb();
		RuleDbNacosConfig nacos = config == null || config.getNacos() == null
				? new RuleDbNacosConfig() : config.getNacos();
		String applicationName = config == null ? null : config.getApplicationName();
		if (StrUtil.isBlank(applicationName)) {
			applicationName = "default";
		}
		this.connection = new NacosConnectionManager(nacos);
		try {
			NacosConfigKey key = new NacosConfigKey(nacos.getDataIdPrefix(), applicationName, nacos.getGroup());
			NacosCatalogCodec codec = new NacosCatalogCodec();
			NacosConfigFacade facade = new ClientNacosConfigFacade(connection.client());
			NacosCatalogStore store = new NacosCatalogStore(facade, key, nacos.getTimeoutMillis(), codec);
			this.repository = new NacosRuleRepository(store);
			this.changeSource = new NacosListenerChangeSource(store);
		}
		catch (RuntimeException | Error e) {
			connection.close();
			throw e;
		}
	}

	@Override
	public String type() {
		return "nacos";
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
		try {
			changeSource.close();
		}
		finally {
			connection.close();
		}
	}
}
