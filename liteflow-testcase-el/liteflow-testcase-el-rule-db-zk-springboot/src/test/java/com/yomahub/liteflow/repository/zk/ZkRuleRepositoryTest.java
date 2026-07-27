package com.yomahub.liteflow.repository.zk;

import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.PublishScriptRequest;
import com.yomahub.liteflow.publisher.exception.RuleStorageException;
import com.yomahub.liteflow.repository.vo.ChainRecord;
import com.yomahub.liteflow.repository.vo.RuleManifest;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.framework.api.GetDataBuilder;
import org.apache.curator.framework.api.ExistsBuilder;
import org.apache.curator.framework.api.WatchPathable;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Repository reads against a real ZooKeeper server plus mock-driven edge cases. */
@SuppressWarnings({ "unchecked", "rawtypes" })
class ZkRuleRepositoryTest {

	private TestingServer server;
	private CuratorFramework client;
	private ZkPaths paths;
	private ZkRecordCodec codec;
	private ZkRuleRepository repository;

	@BeforeEach
	void setUp() throws Exception {
		server = ZkTestSupport.server();
		client = ZkTestSupport.client(server, 5000);
		paths = new ZkPaths("/lf", "repo");
		codec = new ZkRecordCodec();
	}

	@AfterEach
	void tearDown() throws Exception {
		if (client != null) { client.close(); }
		if (server != null) { server.close(); }
	}

	@Test
	void fetchManifestIsEmptyWhenRootsAreMissing() {
		repository = new ZkRuleRepository(client, paths, codec);
		RuleManifest manifest = repository.fetchManifest();
		assertTrue(manifest.getChains().isEmpty());
		assertTrue(manifest.getScripts().isEmpty());
		assertEquals(0, manifest.getLatestSeq());
	}

	@Test
	void fetchManifestListsEnabledRulesSortedAndTracksRevision() throws Exception {
		ZkTestSupport.createRoots(client, paths);
		ZkRulePublisher publisher = new ZkRulePublisher(client, paths, codec);
		publisher.publishChain(chain("c2", "THEN(b)"));
		publisher.publishChain(chain("c1", "THEN(a)"));
		publisher.publishScript(PublishScriptRequest.builder()
				.nodeId("s1").script("return 1").type("script").build());
		// a disabled chain must not appear in the manifest
		ChainRecord disabled = ZkTestSupport.chainRecord("c3", 1, "THEN(c)");
		disabled.setEnable(false);
		client.create().forPath(paths.chainMeta("c3"), codec.encodeChainMeta(disabled));

		repository = new ZkRuleRepository(client, paths, codec);
		RuleManifest manifest = repository.fetchManifest();
		assertEquals(2, manifest.getChains().size());
		assertEquals("c1", manifest.getChains().get(0).getChainId());
		assertEquals("c2", manifest.getChains().get(1).getChainId());
		assertEquals(1, manifest.getScripts().size());
		assertEquals("s1", manifest.getScripts().get(0).getNodeId());
		assertTrue(manifest.getLatestSeq() > 0);
	}

	@Test
	void unknownAndDisabledRulesReadAsAbsent() throws Exception {
		ZkTestSupport.createRoots(client, paths);
		repository = new ZkRuleRepository(client, paths, codec);
		assertNull(repository.fetchChain("nope"));
		assertNull(repository.fetchChainMeta("nope"));
		assertNull(repository.fetchScript("nope"));
		assertNull(repository.fetchScriptMeta("nope"));

		ZkRulePublisher publisher = new ZkRulePublisher(client, paths, codec);
		publisher.publishChain(chain("c1", "THEN(a)"));
		ChainRecord disabled = ZkTestSupport.chainRecord("c1", 2, "THEN(a)");
		disabled.setEnable(false);
		client.setData().forPath(paths.chainMeta("c1"), codec.encodeChainMeta(disabled));

		assertNull(repository.fetchChain("c1"));
		assertNull(repository.fetchChainMeta("c1"));
	}

	@Test
	void metadataContentVersionMismatchIsAStorageError() throws Exception {
		ZkTestSupport.createRoots(client, paths);
		ZkRulePublisher publisher = new ZkRulePublisher(client, paths, codec);
		publisher.publishChain(chain("c1", "THEN(a)"));
		client.setData().forPath(paths.chainContent("c1"), codec.encodeContent(99, "THEN(x)"));

		repository = new ZkRuleRepository(client, paths, codec);
		assertThrows(RuleStorageException.class, () -> repository.fetchChain("c1"));
	}

	@Test
	void unstableMetadataDuringReadIsAStorageError() throws Exception {
		AtomicInteger metaReads = new AtomicInteger();
		CuratorFramework mock = readingClient((path, stat) -> {
			if (path.equals(paths.chainMeta("c1"))) {
				int read = metaReads.incrementAndGet();
				if (stat != null) { stat.setVersion(read); }
				return codec.encodeChainMeta(ZkTestSupport.chainRecord("c1", read, "THEN(a)"));
			}
			if (path.equals(paths.chainContent("c1"))) {
				if (stat != null) { stat.setVersion(1); }
				return codec.encodeContent(1, "THEN(a)");
			}
			throw new KeeperException.NoNodeException(path);
		});
		ZkRuleRepository unstable = new ZkRuleRepository(mock, paths, codec);
		assertThrows(RuleStorageException.class, () -> unstable.fetchChain("c1"));
	}

	@Test
	void metadataDisappearingDuringReadIsAStorageError() throws Exception {
		AtomicInteger metaReads = new AtomicInteger();
		CuratorFramework mock = readingClient((path, stat) -> {
			if (path.equals(paths.chainMeta("c1")) && metaReads.incrementAndGet() == 1) {
				if (stat != null) { stat.setVersion(1); }
				return codec.encodeChainMeta(ZkTestSupport.chainRecord("c1", 1, "THEN(a)"));
			}
			if (path.equals(paths.chainContent("c1"))) {
				if (stat != null) { stat.setVersion(1); }
				return codec.encodeContent(1, "THEN(a)");
			}
			throw new KeeperException.NoNodeException(path);
		});
		ZkRuleRepository disappearing = new ZkRuleRepository(mock, paths, codec);
		assertThrows(RuleStorageException.class, () -> disappearing.fetchChain("c1"));
	}

	@Test
	void childDisappearingDuringManifestListingIsSkipped() throws Exception {
		CuratorFramework mock = mock(CuratorFramework.class, RETURNS_DEEP_STUBS);
		ExistsBuilder exists = mock(ExistsBuilder.class);
		when(mock.checkExists()).thenReturn(exists);
		when(exists.forPath(anyString())).thenReturn(new Stat());
		GetChildrenBuilder children = mock(GetChildrenBuilder.class);
		when(mock.getChildren()).thenReturn(children);
		when(children.forPath(anyString())).thenReturn(Collections.singletonList("gone"));
		GetDataBuilder getData = mock(GetDataBuilder.class);
		WatchPathable<byte[]> dataPath = mock(WatchPathable.class);
		when(mock.getData()).thenReturn(getData);
		when(getData.storingStatIn(any(Stat.class))).thenReturn(dataPath);
		when(dataPath.forPath(anyString())).thenThrow(new KeeperException.NoNodeException("gone"));

		ZkRuleRepository racing = new ZkRuleRepository(mock, paths, codec);
		RuleManifest manifest = racing.fetchManifest();
		assertTrue(manifest.getChains().isEmpty());
		assertTrue(manifest.getScripts().isEmpty());
	}

	@Test
	void rootReadFailureIsWrappedAsStorageException() throws Exception {
		CuratorFramework mock = mock(CuratorFramework.class, RETURNS_DEEP_STUBS);
		ExistsBuilder exists = mock(ExistsBuilder.class);
		when(mock.checkExists()).thenReturn(exists);
		when(exists.forPath(anyString())).thenThrow(new RuntimeException("zookeeper down"));

		ZkRuleRepository failing = new ZkRuleRepository(mock, paths, codec);
		assertThrows(RuleStorageException.class, failing::fetchManifest);
	}

	private interface Reader {
		byte[] read(String path, Stat stat) throws Exception;
	}

	private CuratorFramework readingClient(Reader reader) {
		CuratorFramework mock = mock(CuratorFramework.class, RETURNS_DEEP_STUBS);
		GetDataBuilder getData = mock(GetDataBuilder.class);
		WatchPathable<byte[]> dataPath = mock(WatchPathable.class);
		AtomicReference<Stat> requestedStat = new AtomicReference<>();
		when(mock.getData()).thenReturn(getData);
		when(getData.storingStatIn(any(Stat.class))).thenAnswer(invocation -> {
			requestedStat.set(invocation.getArgument(0));
			return dataPath;
		});
		try {
			when(dataPath.forPath(anyString())).thenAnswer(invocation ->
					reader.read(invocation.getArgument(0), requestedStat.get()));
		}
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
		return mock;
	}

	private PublishChainRequest chain(String id, String el) {
		return PublishChainRequest.builder().chainId(id).el(el).build();
	}
}
