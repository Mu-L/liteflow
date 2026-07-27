package com.yomahub.liteflow.repository.zk;

import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.PublishResult;
import com.yomahub.liteflow.publisher.PublishScriptRequest;
import com.yomahub.liteflow.publisher.RemoveRuleRequest;
import com.yomahub.liteflow.publisher.exception.RuleStorageException;
import com.yomahub.liteflow.publisher.exception.RuleValidationException;
import com.yomahub.liteflow.publisher.exception.VersionConflictException;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Publisher behavior against a real ZooKeeper server (TestingServer). */
class ZkPublisherIntegrationTest {

	private TestingServer server;
	private CuratorFramework client;
	private ZkPaths paths;
	private ZkRecordCodec codec;
	private ZkRulePublisher publisher;

	@BeforeEach
	void setUp() throws Exception {
		server = ZkTestSupport.server();
		client = ZkTestSupport.client(server, 5000);
		paths = new ZkPaths("/lf", "publish");
		codec = new ZkRecordCodec();
		publisher = new ZkRulePublisher(client, paths, codec);
	}

	@AfterEach
	void tearDown() throws Exception {
		if (client != null) { client.close(); }
		if (server != null) { server.close(); }
	}

	@Test
	void publishChainCreatesThenUpdatesAndReadsBack() {
		PublishResult created = publisher.publishChain(chain("c1", "THEN(a)", null));
		assertEquals(1, created.getVersion());
		assertEquals(ChangeRecord.Op.UPSERT, created.getOperation());
		assertTrue(created.getSequence() > 0);

		PublishResult updated = publisher.publishChain(chain("c1", "THEN(a, b)", null));
		assertEquals(2, updated.getVersion());

		ZkRuleRepository repository = new ZkRuleRepository(client, paths, codec);
		assertEquals("THEN(a, b)", repository.fetchChain("c1").getEl());
		assertEquals(2, repository.fetchChainMeta("c1").getVersion());
	}

	@Test
	void publishScriptRoundTripsAllFields() {
		PublishScriptRequest request = PublishScriptRequest.builder()
				.nodeId("s1").script("return 1").name("script one").type("script")
				.language("groovy").build();
		PublishResult result = publisher.publishScript(request);
		assertEquals(1, result.getVersion());
		assertEquals(ChangeRecord.TargetType.SCRIPT, result.getTargetType());

		ZkRuleRepository repository = new ZkRuleRepository(client, paths, codec);
		assertEquals("return 1", repository.fetchScript("s1").getScript());
		assertEquals("groovy", repository.fetchScript("s1").getLanguage());
		assertEquals("script one", repository.fetchScriptMeta("s1").getName());
	}

	@Test
	void staleExpectedVersionIsRejectedBeforeWriting() {
		publisher.publishChain(chain("c1", "THEN(a)", null));
		assertThrows(VersionConflictException.class,
				() -> publisher.publishChain(chain("c1", "THEN(b)", 0L)));
		assertThrows(VersionConflictException.class,
				() -> publisher.publishChain(chain("c1", "THEN(b)", 5L)));
		PublishResult result = publisher.publishChain(chain("c1", "THEN(b)", 1L));
		assertEquals(2, result.getVersion());
	}

	@Test
	void removeChainDeletesAndIsIdempotent() {
		publisher.publishChain(chain("c1", "THEN(a)", null));
		ZkRuleRepository repository = new ZkRuleRepository(client, paths, codec);

		PublishResult removed = publisher.removeChain(remove("c1", 1L));
		assertEquals(ChangeRecord.Op.DELETE, removed.getOperation());
		assertEquals(1, removed.getVersion());
		assertNull(repository.fetchChain("c1"));

		PublishResult again = publisher.removeChain(remove("c1", null));
		assertEquals(0, again.getVersion());
		assertEquals(ChangeRecord.Op.DELETE, again.getOperation());
	}

	@Test
	void removeWithMismatchedExpectedVersionConflicts() {
		publisher.publishChain(chain("c1", "THEN(a)", null));
		assertThrows(VersionConflictException.class, () -> publisher.removeChain(remove("c1", 0L)));
		assertThrows(VersionConflictException.class, () -> publisher.removeChain(remove("c1", 7L)));
	}

	@Test
	void removeScriptDeletesScript() {
		publisher.publishScript(PublishScriptRequest.builder()
				.nodeId("s1").script("return 1").type("script").build());
		publisher.removeScript(remove("s1", 1L));
		ZkRuleRepository repository = new ZkRuleRepository(client, paths, codec);
		assertNull(repository.fetchScript("s1"));
		assertNull(repository.fetchScriptMeta("s1"));
	}

	@Test
	void missingContentDuringUpdateIsAStorageError() throws Exception {
		publisher.publishChain(chain("c1", "THEN(a)", null));
		client.delete().forPath(paths.chainContent("c1"));

		assertThrows(RuleStorageException.class,
				() -> publisher.publishChain(chain("c1", "THEN(b)", null)));
	}

	@Test
	void oversizedPayloadIsRejectedBeforeWriting() {
		String huge = new String(new char[970 * 1024]).replace('\0', 'a');
		RuleValidationException chainError = assertThrows(RuleValidationException.class,
				() -> publisher.publishChain(chain("big", huge, null)));
		assertTrue(chainError.getMessage().contains("byte safety limit"));

		assertThrows(RuleValidationException.class,
				() -> publisher.publishScript(PublishScriptRequest.builder()
						.nodeId("big").script(huge).type("script").build()));

		ZkRuleRepository repository = new ZkRuleRepository(client, paths, codec);
		assertNull(repository.fetchChainMeta("big"));
	}

	@Test
	void constructionFailureDoesNotCloseBorrowedClient() {
		ZkPublisherConfig config = ZkPublisherConfig.builder().applicationName("bad/app")
				.rootPath("/lf").client(client).build();

		assertThrows(RuntimeException.class, () -> new ZkRulePublisher(config));
		assertEquals(CuratorFrameworkState.STARTED, client.getState());
	}

	private PublishChainRequest chain(String id, String el, Long expected) {
		return PublishChainRequest.builder().chainId(id).el(el).expectedVersion(expected).build();
	}

	private RemoveRuleRequest remove(String id, Long expected) {
		return RemoveRuleRequest.builder().targetId(id).expectedVersion(expected).build();
	}
}
