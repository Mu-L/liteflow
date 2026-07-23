package com.yomahub.liteflow.repository.zk;

import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.PublishResult;
import com.yomahub.liteflow.publisher.RulePublisher;
import com.yomahub.liteflow.publisher.exception.PublisherConfigurationException;
import com.yomahub.liteflow.publisher.exception.RuleStorageException;
import com.yomahub.liteflow.publisher.exception.VersionConflictException;
import com.yomahub.liteflow.property.RuleDbZkConfig;
import com.yomahub.liteflow.repository.vo.ChainRecord;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.GetDataBuilder;
import org.apache.curator.framework.api.WatchPathable;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.api.transaction.CuratorMultiTransaction;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.framework.api.transaction.OperationType;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({ "unchecked", "rawtypes" })
class ZkMockContractTest {

	@Test
	void providerUsesBorrowedCuratorMockWithoutStartingOrClosingIt() throws Exception {
		CuratorFramework client = client(Collections.emptyMap());
		ZkRulePublisherProvider provider = new ZkRulePublisherProvider();
		ZkPublisherConfig config = ZkPublisherConfig.builder().applicationName("app")
				.rootPath("/lf").client(client).build();

		assertTrue(provider.supports(config));
		try (RulePublisher ignored = provider.create(config)) {
			verify(client, times(4)).createContainers(anyString());
		}
		verify(client, never()).start();
		verify(client, never()).close();
	}

	@Test
	void providerRejectsMissingClientAndBlankRootBeforeAnyConnection() {
		ZkRulePublisherProvider provider = new ZkRulePublisherProvider();
		assertThrows(PublisherConfigurationException.class,
				() -> provider.create(ZkPublisherConfig.builder().applicationName("app").build()));

		CuratorFramework client = mock(CuratorFramework.class);
		assertThrows(PublisherConfigurationException.class,
				() -> provider.create(ZkPublisherConfig.builder().applicationName("app")
						.client(client).rootPath(" ").build()));
		verify(client, never()).start();
	}

	@Test
	void connectionManagerRejectsInvalidDigestCredentialsBeforeConnecting() {
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

	@Test
	void pathsSeparateMetadataAndContentAndRejectNestedIds() {
		ZkPaths paths = new ZkPaths("/liteflow/", "order-app");
		assertEquals("/liteflow/order-app/chains/meta/c1", paths.chainMeta("c1"));
		assertEquals("/liteflow/order-app/chains/content/c1", paths.chainContent("c1"));
		assertEquals("/liteflow/order-app/scripts/meta", paths.scriptMetaRoot());
		assertThrows(IllegalArgumentException.class, () -> paths.scriptMeta("a/b"));
	}

	@Test
	void codecRoundTripsMetadataAndRejectsVersionMismatch() {
		ZkRecordCodec codec = new ZkRecordCodec();
		ChainRecord record = chainRecord(2, "THEN(a)");
		byte[] metadata = codec.encodeChainMeta(record);
		byte[] content = codec.encodeContent(2, record.getEl());

		ChainRecord decoded = codec.decodeChain("c1", metadata, content);
		assertEquals(2, decoded.getVersion());
		assertEquals("THEN(a)", decoded.getEl());
		assertEquals("route", decoded.getRoute());
		assertEquals("ns", decoded.getNamespace());
		assertThrows(RuleStorageException.class,
				() -> codec.decodeChain("c1", metadata, codec.encodeContent(3, "THEN(b)")));
	}

	@Test
	void publisherCreatesBothZnodesInOneMockTransaction() throws Exception {
		ZkPaths paths = new ZkPaths("/lf", "app");
		CuratorFramework client = client(Collections.emptyMap());
		CuratorTransactionResult transactionResult = mock(CuratorTransactionResult.class);
		Stat committed = new Stat();
		committed.setMzxid(17L);
		when(transactionResult.getForPath()).thenReturn(paths.chainMeta("c1"));
		when(transactionResult.getType()).thenReturn(OperationType.SET_DATA);
		when(transactionResult.getResultStat()).thenReturn(committed);
		CuratorMultiTransaction transaction = client.transaction();
		when(transaction.forOperations(any(List.class)))
				.thenReturn(Collections.singletonList(transactionResult));

		PublishResult result;
		try (RulePublisher publisher = new ZkRulePublisher(client, paths, new ZkRecordCodec())) {
			result = publisher.publishChain(chain("c1", "THEN(a)", 0L));
		}

		assertEquals(1, result.getVersion());
		assertEquals(17, result.getSequence());
		verify(transaction).forOperations(any(List.class));
	}

	@Test
	void staleVersionStopsBeforeTransaction() throws Exception {
		ZkPaths paths = new ZkPaths("/lf", "app");
		ZkRecordCodec codec = new ZkRecordCodec();
		Map<String, byte[]> values = new LinkedHashMap<>();
		values.put(paths.chainMeta("c1"), codec.encodeChainMeta(chainRecord(3, "THEN(a)")));
		CuratorFramework client = client(values);

		try (RulePublisher publisher = new ZkRulePublisher(client, paths, codec)) {
			assertThrows(VersionConflictException.class,
					() -> publisher.publishChain(chain("c1", "THEN(b)", 2L)));
		}
		verify(client, never()).transaction();
	}

	@Test
	void repositoryReadsStablePairAndRejectsMissingContent() throws Exception {
		ZkPaths paths = new ZkPaths("/lf", "app");
		ZkRecordCodec codec = new ZkRecordCodec();
		ChainRecord record = chainRecord(2, "THEN(a)");
		Map<String, byte[]> values = new LinkedHashMap<>();
		values.put(paths.chainMeta("c1"), codec.encodeChainMeta(record));
		values.put(paths.chainContent("c1"), codec.encodeContent(2, record.getEl()));
		ZkRuleRepository repository = new ZkRuleRepository(client(values), paths, codec);

		assertEquals("THEN(a)", repository.fetchChain("c1").getEl());

		values.remove(paths.chainContent("c1"));
		repository = new ZkRuleRepository(client(values), paths, codec);
		ZkRuleRepository missingContent = repository;
		assertThrows(RuleStorageException.class, () -> missingContent.fetchChain("c1"));
	}

	private CuratorFramework client(Map<String, byte[]> values) {
		CuratorFramework client = mock(CuratorFramework.class, RETURNS_DEEP_STUBS);
		GetDataBuilder getData = mock(GetDataBuilder.class);
		WatchPathable<byte[]> dataPath = mock(WatchPathable.class);
		AtomicReference<Stat> requestedStat = new AtomicReference<>();
		when(client.getState()).thenReturn(CuratorFrameworkState.STARTED);
		try {
			when(client.blockUntilConnected(anyInt(), any(TimeUnit.class))).thenReturn(true);
		}
		catch (InterruptedException e) {
			throw new IllegalStateException(e);
		}
		when(client.getData()).thenReturn(getData);
		when(getData.storingStatIn(any(Stat.class))).thenAnswer(invocation -> {
			requestedStat.set(invocation.getArgument(0));
			return dataPath;
		});
		try {
			when(dataPath.forPath(anyString())).thenAnswer(invocation -> {
				String path = invocation.getArgument(0);
				byte[] value = values.get(path);
				if (value == null) { throw new KeeperException.NoNodeException(path); }
				Stat stat = requestedStat.get();
				if (stat != null) {
					stat.setVersion(1);
					stat.setMzxid(11L);
				}
				return value;
			});
		}
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
		return client;
	}

	private ChainRecord chainRecord(long version, String el) {
		ChainRecord record = new ChainRecord();
		record.setChainId("c1");
		record.setVersion(version);
		record.setMd5("md5");
		record.setEnable(true);
		record.setRoute("route");
		record.setNamespace("ns");
		record.setEl(el);
		return record;
	}

	private PublishChainRequest chain(String id, String el, Long expected) {
		return PublishChainRequest.builder().chainId(id).el(el).expectedVersion(expected).build();
	}
}
