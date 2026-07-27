package com.yomahub.liteflow.repository.zk;

import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.property.LiteflowConfig;
import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.property.RuleDbZkConfig;
import com.yomahub.liteflow.repository.vo.RuleManifest;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Execution-side provider wiring against a real ZooKeeper server. */
class ZkRuleDbProviderTest {

	private TestingServer server;
	private CuratorFramework inspector;

	@BeforeEach
	void setUp() throws Exception {
		server = ZkTestSupport.server();
		inspector = ZkTestSupport.client(server, 5000);
	}

	@AfterEach
	void tearDown() throws Exception {
		LiteflowConfigGetter.clean();
		if (inspector != null) { inspector.close(); }
		if (server != null) { server.close(); }
	}

	@Test
	void providerUsesExistingRootsAndExposesRepositoryAndChangeSource() throws Exception {
		ZkTestSupport.createRoots(inspector, new ZkPaths("/liteflow", "it-app"));
		configure("it-app");
		ZkRuleDbProvider provider = new ZkRuleDbProvider();
		try {
			assertEquals("zk", provider.type());
			assertNotNull(provider.repository());
			assertNotNull(provider.changeSource());

			RuleManifest manifest = provider.repository().fetchManifest();
			assertTrue(manifest.getChains().isEmpty());
			assertNotNull(inspector.checkExists().forPath("/liteflow/it-app/chains/meta"));
			assertNotNull(inspector.checkExists().forPath("/liteflow/it-app/scripts/content"));
		}
		finally {
			provider.close();
			provider.close();
		}
	}

	@Test
	void blankApplicationNameFallsBackToDefault() throws Exception {
		ZkTestSupport.createRoots(inspector, new ZkPaths("/liteflow", "default"));
		configure(null);
		ZkRuleDbProvider provider = new ZkRuleDbProvider();
		try {
			assertNotNull(inspector.checkExists().forPath("/liteflow/default/chains/meta"));
		}
		finally {
			provider.close();
		}
	}

	@Test
	void missingRootsFailWithPublisherInitializationGuidance() {
		configure("missing");
		ConfigErrorException error = assertThrows(ConfigErrorException.class, ZkRuleDbProvider::new);
		assertTrue(error.getMessage().contains("does not exist"));
		assertTrue(error.getMessage().contains("rule publisher"));
	}

	@Test
	void illegalApplicationNameFailsFastAtStartup() {
		configure("bad/app");
		assertThrows(ConfigErrorException.class, ZkRuleDbProvider::new);
	}

	@Test
	void missingConnectionConfigurationFailsFast() {
		RuleDbZkConfig zk = new RuleDbZkConfig();
		zk.setConnectString(null);
		zk.setCuratorBeanName(null);
		configure("app", zk);
		assertThrows(ConfigErrorException.class, ZkRuleDbProvider::new);
	}

	@Test
	void readOnlyIdentityCanStartProviderButCannotCreateNodes() throws Exception {
		String applicationName = "readonly";
		ZkPaths paths = new ZkPaths("/liteflow", applicationName);
		ZkPublisherConfig publisherConfig = ZkPublisherConfig.builder().applicationName(applicationName)
				.connectString(server.getConnectString()).username("admin").password("secret").build();
		try (ZkRulePublisher ignored = new ZkRulePublisher(publisherConfig)) {
			// Publisher owns schema initialization.
		}

		try (CuratorFramework admin = authenticatedClient("admin", "secret")) {
			List<ACL> acls = Arrays.asList(
					new ACL(ZooDefs.Perms.ALL, digestId("admin", "secret")),
					new ACL(ZooDefs.Perms.READ, digestId("reader", "secret")));
			for (String path : securedPaths(applicationName, paths)) {
				admin.setACL().withACL(acls).forPath(path);
			}
		}

		RuleDbZkConfig zk = new RuleDbZkConfig();
		zk.setConnectString(server.getConnectString());
		zk.setUsername("reader");
		zk.setPassword("secret");
		configure(applicationName, zk);
		ZkRuleDbProvider provider = new ZkRuleDbProvider();
		try {
			assertTrue(provider.repository().fetchManifest().getChains().isEmpty());
		}
		finally {
			provider.close();
		}

		try (CuratorFramework reader = authenticatedClient("reader", "secret")) {
			assertThrows(KeeperException.NoAuthException.class,
					() -> reader.create().forPath(paths.chainMetaRoot() + "/forbidden"));
		}
	}

	private void configure(String applicationName) {
		RuleDbZkConfig zk = new RuleDbZkConfig();
		zk.setConnectString(server.getConnectString());
		configure(applicationName, zk);
	}

	private void configure(String applicationName, RuleDbZkConfig zk) {
		RuleDbConfig ruleDb = new RuleDbConfig();
		ruleDb.setApplicationName(applicationName);
		ruleDb.setZk(zk);
		LiteflowConfig config = new LiteflowConfig();
		config.setRuleDb(ruleDb);
		LiteflowConfigGetter.setLiteflowConfig(config);
	}

	private CuratorFramework authenticatedClient(String username, String password) throws Exception {
		CuratorFramework client = CuratorFrameworkFactory.builder()
				.connectString(server.getConnectString()).sessionTimeoutMs(5000).connectionTimeoutMs(3000)
				.retryPolicy(new RetryOneTime(50))
				.authorization("digest", (username + ":" + password).getBytes(StandardCharsets.UTF_8))
				.build();
		client.start();
		assertTrue(client.blockUntilConnected(5, java.util.concurrent.TimeUnit.SECONDS));
		return client;
	}

	private Id digestId(String username, String password) throws Exception {
		return new Id("digest", DigestAuthenticationProvider.generateDigest(username + ":" + password));
	}

	private String[] securedPaths(String applicationName, ZkPaths paths) {
		String base = "/liteflow/" + applicationName;
		return new String[] { "/liteflow", base, base + "/chains", paths.chainMetaRoot(),
				paths.chainContentRoot(), base + "/scripts", paths.scriptMetaRoot(), paths.scriptContentRoot() };
	}
}
