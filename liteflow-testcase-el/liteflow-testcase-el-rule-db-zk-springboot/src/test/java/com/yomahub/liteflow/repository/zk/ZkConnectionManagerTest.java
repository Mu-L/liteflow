package com.yomahub.liteflow.repository.zk;

import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.publisher.exception.PublisherConfigurationException;
import com.yomahub.liteflow.property.RuleDbZkConfig;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Connection manager validation and lifecycle against a real ZooKeeper server. */
class ZkConnectionManagerTest {

	@Test
	void sessionTimeoutMustBePositive() {
		RuleDbZkConfig zero = new RuleDbZkConfig();
		zero.setConnectString("127.0.0.1:1");
		zero.setSessionTimeout(0);
		assertThrows(ConfigErrorException.class, () -> new ZkConnectionManager(zero));

		RuleDbZkConfig negative = new RuleDbZkConfig();
		negative.setConnectString("127.0.0.1:1");
		negative.setSessionTimeout(-100);
		assertThrows(ConfigErrorException.class, () -> new ZkConnectionManager(negative));
	}

	@Test
	void ownedClientConnectsAndCloses() throws Exception {
		try (TestingServer server = ZkTestSupport.server()) {
			RuleDbZkConfig config = new RuleDbZkConfig();
			config.setConnectString(server.getConnectString());
			ZkConnectionManager manager = new ZkConnectionManager(config);
			assertEquals(CuratorFrameworkState.STARTED, manager.client().getState());
			manager.client().createContainers("/lf/probe");
			manager.close();
			assertEquals(CuratorFrameworkState.STOPPED, manager.client().getState());
		}
	}

	@Test
	void publisherProviderRejectsBlankApplicationName() {
		ZkRulePublisherProvider provider = new ZkRulePublisherProvider();
		assertThrows(PublisherConfigurationException.class,
				() -> provider.create(ZkPublisherConfig.builder()
						.connectString("127.0.0.1:1").build()));
		assertThrows(PublisherConfigurationException.class,
				() -> provider.create(ZkPublisherConfig.builder()
						.applicationName(" ").connectString("127.0.0.1:1").build()));
	}
}
