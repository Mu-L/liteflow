package com.yomahub.liteflow.repository.zk;

import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.PublishResult;
import com.yomahub.liteflow.publisher.PublishScriptRequest;
import com.yomahub.liteflow.publisher.RemoveRuleRequest;
import com.yomahub.liteflow.publisher.RulePublisher;
import com.yomahub.liteflow.publisher.RulePublisherFactory;
import com.yomahub.liteflow.publisher.exception.RuleStorageException;
import com.yomahub.liteflow.publisher.exception.VersionConflictException;
import com.yomahub.liteflow.repository.vo.RuleManifest;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ZkRuleRepositoryPublisherTest {

	private TestingServer server;
	private CuratorFramework client;
	private ZkPaths paths;
	private ZkRecordCodec codec;
	private ZkRuleRepository repository;
	private RulePublisher publisher;

	@BeforeEach
	void setUp() throws Exception {
		server = new TestingServer(true);
		client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(100));
		client.start();
		assertTrue(client.blockUntilConnected(5, TimeUnit.SECONDS));
		paths = new ZkPaths("/lf", "app");
		codec = new ZkRecordCodec();
		publisher = RulePublisherFactory.create(ZkPublisherConfig.builder()
				.applicationName("app").rootPath("/lf").client(client).build());
		repository = new ZkRuleRepository(client, paths, codec);
	}

	@AfterEach
	void tearDown() throws Exception {
		if (publisher != null) { publisher.close(); }
		if (client != null) { client.close(); }
		if (server != null) { server.close(); }
	}

	@Test
	void manifestDoesNotRequireContentReads() throws Exception {
		PublishResult result = publisher.publishChain(chain("c1", "THEN(a)", 0L));
		client.delete().forPath(paths.chainContent("c1"));

		RuleManifest manifest = repository.fetchManifest();
		assertEquals(1, manifest.getChains().size());
		assertTrue(manifest.getLatestSeq() >= result.getSequence());
		assertThrows(RuleStorageException.class, () -> repository.fetchChain("c1"));
	}

	@Test
	void exactVersionUpdateChangesBothRecords() {
		PublishResult one = publisher.publishChain(chain("c1", "THEN(a)", 0L));
		PublishResult two = publisher.publishChain(chain("c1", "THEN(b)", one.getVersion()));

		assertEquals(2, two.getVersion());
		assertEquals(2, repository.fetchChain("c1").getVersion());
		assertEquals("THEN(b)", repository.fetchChain("c1").getEl());
	}

	@Test
	void conflictLeavesMetadataAndContentUnchanged() throws Exception {
		publisher.publishChain(chain("c1", "THEN(a)", 0L));
		byte[] metadata = client.getData().forPath(paths.chainMeta("c1"));
		byte[] content = client.getData().forPath(paths.chainContent("c1"));

		assertThrows(VersionConflictException.class,
				() -> publisher.publishChain(chain("c1", "THEN(c)", 99L)));
		org.junit.jupiter.api.Assertions.assertArrayEquals(metadata,
				client.getData().forPath(paths.chainMeta("c1")));
		org.junit.jupiter.api.Assertions.assertArrayEquals(content,
				client.getData().forPath(paths.chainContent("c1")));
	}

	@Test
	void rejectsMetadataContentBusinessVersionMismatch() throws Exception {
		publisher.publishChain(chain("c1", "THEN(a)", 0L));
		client.setData().forPath(paths.chainContent("c1"), codec.encodeContent(2, "THEN(b)"));

		assertThrows(RuleStorageException.class, () -> repository.fetchChain("c1"));
	}

	@Test
	void scriptDeleteRemovesBothZnodes() throws Exception {
		PublishResult created = publisher.publishScript(PublishScriptRequest.builder()
				.nodeId("s1").script("return 1").type("script").language("groovy")
				.expectedVersion(0L).build());

		publisher.removeScript(RemoveRuleRequest.builder()
				.targetId("s1").expectedVersion(created.getVersion()).build());

		assertNull(client.checkExists().forPath(paths.scriptMeta("s1")));
		assertNull(client.checkExists().forPath(paths.scriptContent("s1")));
	}

	private PublishChainRequest chain(String id, String el, Long expected) {
		return PublishChainRequest.builder().chainId(id).el(el).expectedVersion(expected).build();
	}
}
