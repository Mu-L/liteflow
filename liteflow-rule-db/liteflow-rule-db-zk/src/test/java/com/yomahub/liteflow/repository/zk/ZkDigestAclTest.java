package com.yomahub.liteflow.repository.zk;

import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.RulePublisher;
import com.yomahub.liteflow.publisher.RulePublisherFactory;
import com.yomahub.liteflow.property.RuleDbZkConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ZkDigestAclTest {

	private TestingServer server;
	private CuratorFramework anonymous;
	private RulePublisher publisher;

	@BeforeEach
	void setUp() throws Exception {
		server = new TestingServer(true);
		anonymous = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(100));
		anonymous.start();
		assertTrue(anonymous.blockUntilConnected(5, TimeUnit.SECONDS));
	}

	@AfterEach
	void tearDown() throws Exception {
		if (publisher != null) { publisher.close(); }
		if (anonymous != null) { anonymous.close(); }
		if (server != null) { server.close(); }
	}

	@Test
	void digestPublisherCreatesOwnerOnlyRuleTree() throws Exception {
		publisher = RulePublisherFactory.create(ZkPublisherConfig.builder()
				.applicationName("secure-app")
				.connectString(server.getConnectString())
				.sessionTimeout(5000)
				.rootPath("/secure-liteflow")
				.username("liteflow")
				.password("secret")
				.build());
		publisher.publishChain(PublishChainRequest.builder()
				.chainId("c1").el("THEN(a)").expectedVersion(0L).build());

		String path = "/secure-liteflow/secure-app/chains/meta/c1";
		assertThrows(KeeperException.NoAuthException.class, () -> anonymous.getData().forPath(path));

		CuratorFramework authenticated = CuratorFrameworkFactory.builder()
				.connectString(server.getConnectString())
				.retryPolicy(new RetryOneTime(100))
				.authorization("digest", "liteflow:secret".getBytes(java.nio.charset.StandardCharsets.UTF_8))
				.build();
		try {
			authenticated.start();
			assertTrue(authenticated.blockUntilConnected(5, TimeUnit.SECONDS));
			List<ACL> acl = authenticated.getACL().forPath(path);
			assertEquals(1, acl.size());
			assertEquals("digest", acl.get(0).getId().getScheme());
			assertEquals(org.apache.zookeeper.ZooDefs.Perms.ALL, acl.get(0).getPerms());
			assertTrue(authenticated.getData().forPath(path).length > 0);
		}
		finally {
			authenticated.close();
		}
	}

	@Test
	void rejectsPartialDigestCredentialsBeforeConnecting() {
		RuleDbZkConfig onlyUser = new RuleDbZkConfig();
		onlyUser.setConnectString("127.0.0.1:1");
		onlyUser.setUsername("liteflow");
		assertThrows(ConfigErrorException.class, () -> new ZkConnectionManager(onlyUser));

		RuleDbZkConfig invalidUser = new RuleDbZkConfig();
		invalidUser.setConnectString("127.0.0.1:1");
		invalidUser.setUsername("bad:user");
		invalidUser.setPassword("secret");
		assertThrows(ConfigErrorException.class, () -> new ZkConnectionManager(invalidUser));
	}
}
