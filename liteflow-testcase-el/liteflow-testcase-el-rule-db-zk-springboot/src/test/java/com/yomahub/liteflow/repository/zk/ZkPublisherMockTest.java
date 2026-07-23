package com.yomahub.liteflow.repository.zk;

import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.RemoveRuleRequest;
import com.yomahub.liteflow.publisher.RulePublisher;
import com.yomahub.liteflow.publisher.exception.RuleStorageException;
import com.yomahub.liteflow.publisher.exception.VersionConflictException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.GetDataBuilder;
import org.apache.curator.framework.api.PathAndBytesable;
import org.apache.curator.framework.api.Pathable;
import org.apache.curator.framework.api.WatchPathable;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.TransactionCreateBuilder;
import org.apache.curator.framework.api.transaction.TransactionDeleteBuilder;
import org.apache.curator.framework.api.transaction.TransactionOp;
import org.apache.curator.framework.api.transaction.TransactionSetDataBuilder;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Mock-driven publisher tests for paths a real ZooKeeper server cannot reach
 * deterministically: CAS retry exhaustion and exception classification.
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
class ZkPublisherMockTest {

	@Test
	void publishRetriesExhaustedFailsWithStorageException() throws Exception {
		ZkPaths paths = new ZkPaths("/lf", "app");
		ZkRecordCodec codec = new ZkRecordCodec();
		CuratorFramework client = conflictingClient(paths, codec);

		try (RulePublisher publisher = new ZkRulePublisher(client, paths, codec)) {
			RuleStorageException error = assertThrows(RuleStorageException.class,
					() -> publisher.publishChain(chain("c1", "THEN(b)", null)));
			assertTrue(error.getMessage().contains("exceeded concurrent retry limit"));
		}
		verify(client.transaction(), times(8)).forOperations(any(List.class));
	}

	@Test
	void removeRetriesExhaustedFailsWithStorageException() throws Exception {
		ZkPaths paths = new ZkPaths("/lf", "app");
		ZkRecordCodec codec = new ZkRecordCodec();
		CuratorFramework client = conflictingClient(paths, codec);

		try (RulePublisher publisher = new ZkRulePublisher(client, paths, codec)) {
			RuleStorageException error = assertThrows(RuleStorageException.class,
					() -> publisher.removeChain(remove("c1", null)));
			assertTrue(error.getMessage().contains("exceeded concurrent retry limit"));
		}
		verify(client.transaction(), times(8)).forOperations(any(List.class));
	}

	@Test
	void conflictWithExpectedVersionRethrowsAsVersionConflict() throws Exception {
		ZkPaths paths = new ZkPaths("/lf", "app");
		ZkRecordCodec codec = new ZkRecordCodec();
		CuratorFramework client = conflictingClient(paths, codec);

		try (RulePublisher publisher = new ZkRulePublisher(client, paths, codec)) {
			assertThrows(VersionConflictException.class,
					() -> publisher.publishChain(chain("c1", "THEN(b)", 1L)));
			assertThrows(VersionConflictException.class,
					() -> publisher.removeChain(remove("c1", 1L)));
		}
	}

	@Test
	void nonConflictExceptionIsWrappedImmediately() throws Exception {
		ZkPaths paths = new ZkPaths("/lf", "app");
		ZkRecordCodec codec = new ZkRecordCodec();
		CuratorFramework client = failingClient(paths, codec, new KeeperException.ConnectionLossException());

		try (RulePublisher publisher = new ZkRulePublisher(client, paths, codec)) {
			assertThrows(RuleStorageException.class,
					() -> publisher.publishChain(chain("c1", "THEN(b)", null)));
		}
		verify(client.transaction(), times(1)).forOperations(any(List.class));
	}

	private CuratorFramework conflictingClient(ZkPaths paths, ZkRecordCodec codec) throws Exception {
		return failingClient(paths, codec, new KeeperException.BadVersionException(paths.chainMeta("c1")));
	}

	private CuratorFramework failingClient(ZkPaths paths, ZkRecordCodec codec, Exception failure) throws Exception {
		Map<String, byte[]> values = new LinkedHashMap<>();
		values.put(paths.chainMeta("c1"), codec.encodeChainMeta(ZkTestSupport.chainRecord("c1", 1, "THEN(a)")));
		values.put(paths.chainContent("c1"), codec.encodeContent(1, "THEN(a)"));
		CuratorFramework client = client(values);
		when(client.transaction().forOperations(any(List.class))).thenThrow(failure);
		return client;
	}

	private CuratorFramework client(Map<String, byte[]> values) {
		CuratorFramework client = mock(CuratorFramework.class, RETURNS_DEEP_STUBS);
		GetDataBuilder getData = mock(GetDataBuilder.class);
		WatchPathable<byte[]> dataPath = mock(WatchPathable.class);
		AtomicReference<Stat> requestedStat = new AtomicReference<>();
		when(client.getState()).thenReturn(CuratorFrameworkState.STARTED);
		try {
			when(client.blockUntilConnected(anyInt(), any(TimeUnit.class))).thenReturn(true);
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
		when(client.getData()).thenReturn(getData);
		when(getData.storingStatIn(any(Stat.class))).thenAnswer(invocation -> {
			requestedStat.set(invocation.getArgument(0));
			return dataPath;
		});
		stubTransactionOps(client);
		return client;
	}

	private void stubTransactionOps(CuratorFramework client) {
		try {
			CuratorOp op = mock(CuratorOp.class);
			TransactionOp transactionOp = mock(TransactionOp.class);
			TransactionCreateBuilder<CuratorOp> create = mock(TransactionCreateBuilder.class);
			TransactionSetDataBuilder<CuratorOp> setData = mock(TransactionSetDataBuilder.class);
			TransactionDeleteBuilder<CuratorOp> delete = mock(TransactionDeleteBuilder.class);
			PathAndBytesable<CuratorOp> setDataTerminal = mock(PathAndBytesable.class);
			Pathable<CuratorOp> deleteTerminal = mock(Pathable.class);
			when(client.transactionOp()).thenReturn(transactionOp);
			when(transactionOp.create()).thenReturn(create);
			when(create.forPath(anyString(), any(byte[].class))).thenReturn(op);
			when(transactionOp.setData()).thenReturn(setData);
			when(setData.withVersion(anyInt())).thenReturn(setDataTerminal);
			when(setDataTerminal.forPath(anyString(), any(byte[].class))).thenReturn(op);
			when(transactionOp.delete()).thenReturn(delete);
			when(delete.withVersion(anyInt())).thenReturn(deleteTerminal);
			when(deleteTerminal.forPath(anyString())).thenReturn(op);
		}
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	private PublishChainRequest chain(String id, String el, Long expected) {
		return PublishChainRequest.builder().chainId(id).el(el).expectedVersion(expected).build();
	}

	private RemoveRuleRequest remove(String id, Long expected) {
		return RemoveRuleRequest.builder().targetId(id).expectedVersion(expected).build();
	}
}
