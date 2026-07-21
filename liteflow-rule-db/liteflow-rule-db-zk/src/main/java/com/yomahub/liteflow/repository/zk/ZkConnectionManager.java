package com.yomahub.liteflow.repository.zk;

import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.property.RuleDbZkConfig;
import com.yomahub.liteflow.spi.holder.ContextAwareHolder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

final class ZkConnectionManager implements AutoCloseable {

	private final CuratorFramework client;
	private final boolean owned;

	ZkConnectionManager(RuleDbZkConfig config) {
		this(executionClient(config), config == null ? null : config.getConnectString(),
				config == null ? null : config.getSessionTimeout(),
				config == null ? null : config.getUsername(), config == null ? null : config.getPassword());
	}

	ZkConnectionManager(ZkPublisherConfig config) {
		this(config.getClient(), config.getConnectString(), config.getSessionTimeout(),
				config.getUsername(), config.getPassword());
	}

	private ZkConnectionManager(CuratorFramework supplied, String connectString, Integer sessionTimeout,
			String username, String password) {
		if (supplied != null) {
			this.client = supplied;
			this.owned = false;
		}
		else {
			if (StrUtil.isBlank(connectString)) {
				throw new ConfigErrorException("rule-db zk connectString must not be blank");
				}
				int session = sessionTimeout == null ? 60000 : sessionTimeout;
				validateCredentials(username, password);
				CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder()
						.connectString(connectString)
						.sessionTimeoutMs(session)
						.connectionTimeoutMs(Math.min(session, 15000))
						.retryPolicy(new RetryNTimes(5, 1000));
				if (StrUtil.isNotBlank(username)) {
					builder.authorization("digest", (username + ":" + password).getBytes(StandardCharsets.UTF_8))
							.aclProvider(creatorAclProvider());
				}
				this.client = builder.build();
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

	private static CuratorFramework executionClient(RuleDbZkConfig config) {
		if (config == null || StrUtil.isBlank(config.getCuratorBeanName())) {
			return null;
		}
		try {
			return ContextAwareHolder.loadContextAware().getBean(config.getCuratorBeanName());
		}
		catch (Exception e) {
			throw new ConfigErrorException("rule-db zk CuratorFramework bean["
					+ config.getCuratorBeanName() + "] is not available");
		}
	}

	private static void validateCredentials(String username, String password) {
		if (StrUtil.isBlank(username) != StrUtil.isBlank(password)) {
			throw new ConfigErrorException("rule-db zk username and password must be configured together");
		}
		if (StrUtil.isNotBlank(username) && username.indexOf(':') >= 0) {
			throw new ConfigErrorException("rule-db zk username must not contain ':'");
		}
	}

	private static ACLProvider creatorAclProvider() {
		return new ACLProvider() {
			@Override
			public List<ACL> getDefaultAcl() {
				return ZooDefs.Ids.CREATOR_ALL_ACL;
			}

			@Override
			public List<ACL> getAclForPath(String path) {
				return ZooDefs.Ids.CREATOR_ALL_ACL;
			}
		};
	}
}
