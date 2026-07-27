package com.yomahub.liteflow.repository.zk;

import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.property.RuleDbZkConfig;
import com.yomahub.liteflow.repository.RuleChangeSource;
import com.yomahub.liteflow.repository.RuleDbProvider;
import com.yomahub.liteflow.repository.RuleRepository;
import org.apache.curator.framework.CuratorFramework;

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
		try {
			ZkPaths paths = new ZkPaths(zk.getRootPath(), applicationName);
			validateRoots(connection.client(), paths);
			ZkRecordCodec codec = new ZkRecordCodec();
			this.repository = new ZkRuleRepository(connection.client(), paths, codec);
			this.changeSource = new ZkCacheChangeSource(connection.client(), paths, codec);
		}
		catch (RuntimeException e) {
			connection.close();
			throw e;
		}
	}

	@Override public String type() { return "zk"; }
	@Override public RuleRepository repository() { return repository; }
	@Override public RuleChangeSource changeSource() { return changeSource; }

	@Override
	public void close() {
		changeSource.close();
		connection.close();
	}

	private static void validateRoots(CuratorFramework client, ZkPaths paths) {
		String[] requiredRoots = { paths.chainMetaRoot(), paths.chainContentRoot(),
				paths.scriptMetaRoot(), paths.scriptContentRoot() };
		for (String root : requiredRoots) {
			try {
				if (client.checkExists().forPath(root) == null) {
					throw new ConfigErrorException("rule-db zk required root[" + root
							+ "] does not exist; initialize it with the ZooKeeper rule publisher");
				}
			}
			catch (ConfigErrorException e) {
				throw e;
			}
			catch (Exception e) {
				throw new ConfigErrorException("rule-db zk cannot verify required root[" + root
						+ "]: " + e.getMessage());
			}
		}
	}
}
