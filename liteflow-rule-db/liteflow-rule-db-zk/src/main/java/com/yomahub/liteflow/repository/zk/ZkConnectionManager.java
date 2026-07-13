package com.yomahub.liteflow.repository.zk;

import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.property.RuleDbZkConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.RetryNTimes;

import java.util.concurrent.TimeUnit;

final class ZkConnectionManager implements AutoCloseable {

	private final CuratorFramework client;
	private final boolean owned;

	ZkConnectionManager(RuleDbZkConfig config) {
		this(null, config == null ? null : config.getConnectString(),
				config == null ? null : config.getSessionTimeout());
	}

	ZkConnectionManager(ZkPublisherConfig config) {
		this(config.getClient(), config.getConnectString(), config.getSessionTimeout());
	}

	private ZkConnectionManager(CuratorFramework supplied, String connectString, Integer sessionTimeout) {
		if (supplied != null) {
			this.client = supplied;
			this.owned = false;
		}
		else {
			if (StrUtil.isBlank(connectString)) {
				throw new ConfigErrorException("rule-db zk connectString must not be blank");
			}
			int session = sessionTimeout == null ? 60000 : sessionTimeout;
			this.client = CuratorFrameworkFactory.builder()
					.connectString(connectString)
					.sessionTimeoutMs(session)
					.connectionTimeoutMs(Math.min(session, 15000))
					.retryPolicy(new RetryNTimes(5, 1000))
					.build();
			this.owned = true;
		}
		startAndAwait();
	}

	CuratorFramework client() { return client; }

	@Override
	public void close() {
		if (owned) { client.close(); }
	}

	private void startAndAwait() {
		try {
			if (client.getState() == CuratorFrameworkState.LATENT) {
				client.start();
			}
			if (!client.blockUntilConnected(15, TimeUnit.SECONDS)) {
				throw new ConfigErrorException("rule-db zk connection timed out");
			}
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ConfigErrorException("rule-db zk connection interrupted");
		}
	}
}
