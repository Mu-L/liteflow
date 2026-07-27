package com.yomahub.liteflow.repository.zk;

import cn.hutool.crypto.SecureUtil;
import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.PublishResult;
import com.yomahub.liteflow.publisher.PublishScriptRequest;
import com.yomahub.liteflow.publisher.RemoveRuleRequest;
import com.yomahub.liteflow.publisher.RulePublisher;
import com.yomahub.liteflow.publisher.exception.RuleStorageException;
import com.yomahub.liteflow.publisher.exception.RuleValidationException;
import com.yomahub.liteflow.publisher.exception.VersionConflictException;
import com.yomahub.liteflow.repository.vo.ChainRecord;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import com.yomahub.liteflow.repository.vo.ScriptRecord;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.framework.api.transaction.OperationType;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.List;

final class ZkRulePublisher implements RulePublisher {

	private static final int MAX_UNCONDITIONAL_RETRIES = 8;

	/** Safety margin below the ZooKeeper 1MB jute.maxbuffer znode limit. */
	private static final int MAX_ZNODE_BYTES = 960 * 1024;

	private final ZkConnectionManager connection;
	private final CuratorFramework client;
	private final ZkPaths paths;
	private final ZkRecordCodec codec;

	ZkRulePublisher(ZkPublisherConfig config) {
		this.connection = new ZkConnectionManager(config);
		try {
			this.client = connection.client();
			this.paths = new ZkPaths(config.getRootPath(), config.applicationName());
			this.codec = new ZkRecordCodec();
			ensureRoots();
		}
		catch (RuntimeException e) {
			connection.close();
			throw e;
		}
	}

	ZkRulePublisher(CuratorFramework client, ZkPaths paths, ZkRecordCodec codec) {
		this.connection = null;
		this.client = client;
		this.paths = paths;
		this.codec = codec;
		ensureRoots();
	}

	@Override
	public PublishResult publishChain(PublishChainRequest request) {
		return publish(request.getChainId(), ChangeRecord.TargetType.CHAIN, request.getExpectedVersion(),
				paths.chainMeta(request.getChainId()), paths.chainContent(request.getChainId()), version -> {
					ChainRecord record = new ChainRecord();
					record.setChainId(request.getChainId());
					record.setEl(request.getEl());
					record.setRoute(request.getRoute());
					record.setNamespace(request.getNamespace());
					record.setVersion(version);
					record.setMd5(SecureUtil.md5(request.getEl()));
					record.setEnable(true);
					return new Encoded(codec.encodeChainMeta(record), codec.encodeContent(version, request.getEl()));
				});
	}

	@Override
	public PublishResult publishScript(PublishScriptRequest request) {
		return publish(request.getNodeId(), ChangeRecord.TargetType.SCRIPT, request.getExpectedVersion(),
				paths.scriptMeta(request.getNodeId()), paths.scriptContent(request.getNodeId()), version -> {
					ScriptRecord record = new ScriptRecord();
					record.setNodeId(request.getNodeId());
					record.setScript(request.getScript());
					record.setName(request.getName());
					record.setType(request.getType());
					record.setLanguage(request.getLanguage());
					record.setVersion(version);
					record.setMd5(SecureUtil.md5(request.getScript()));
					record.setEnable(true);
					return new Encoded(codec.encodeScriptMeta(record), codec.encodeContent(version, request.getScript()));
				});
	}

	@Override
	public PublishResult removeChain(RemoveRuleRequest request) {
		return remove(request, ChangeRecord.TargetType.CHAIN,
				paths.chainMeta(request.getTargetId()), paths.chainContent(request.getTargetId()), paths.chainMetaRoot());
	}

	@Override
	public PublishResult removeScript(RemoveRuleRequest request) {
		return remove(request, ChangeRecord.TargetType.SCRIPT,
				paths.scriptMeta(request.getTargetId()), paths.scriptContent(request.getTargetId()), paths.scriptMetaRoot());
	}

	@Override
	public void close() {
		if (connection != null) { connection.close(); }
	}

	private PublishResult publish(String id, ChangeRecord.TargetType type, Long expected,
			String metadataPath, String contentPath, Encoder encoder) {
		for (int attempt = 0; attempt < MAX_UNCONDITIONAL_RETRIES; attempt++) {
			Data metadata = get(metadataPath);
			long currentVersion = metadata == null ? 0 : codec.version(metadata.value);
			if (expected != null && expected != currentVersion) { throw conflict(type, id, expected, currentVersion); }
			long nextVersion = currentVersion + 1;
			Encoded encoded = encoder.encode(nextVersion);
			ensureWithinLimit(type, id, encoded);
			try {
				List<CuratorOp> operations = new ArrayList<>();
				if (metadata == null) {
					operations.add(client.transactionOp().create().forPath(contentPath, encoded.content));
					operations.add(client.transactionOp().create().forPath(metadataPath, encoded.metadata));
				}
				else {
					Data content = get(contentPath);
					if (content == null) { throw new RuleStorageException("ZooKeeper content is missing for " + metadataPath); }
					operations.add(client.transactionOp().setData().withVersion(content.stat.getVersion())
							.forPath(contentPath, encoded.content));
					operations.add(client.transactionOp().setData().withVersion(metadata.stat.getVersion())
							.forPath(metadataPath, encoded.metadata));
				}
				List<CuratorTransactionResult> results = client.transaction().forOperations(operations);
				long sequence = metadataRevision(results, metadataPath);
				return result(id, type, ChangeRecord.Op.UPSERT, nextVersion, sequence);
			}
			catch (Exception e) {
				if (!isConflict(e)) { throw storage("publish " + type.name().toLowerCase() + "[" + id + "]", e); }
				if (expected != null) { throw conflict(type, id, expected, currentVersion(metadataPath)); }
			}
		}
		throw new RuleStorageException("ZooKeeper publish " + type.name().toLowerCase()
				+ "[" + id + "] exceeded concurrent retry limit");
	}

	private PublishResult remove(RemoveRuleRequest request, ChangeRecord.TargetType type,
			String metadataPath, String contentPath, String metadataRoot) {
		for (int attempt = 0; attempt < MAX_UNCONDITIONAL_RETRIES; attempt++) {
			Data metadata = get(metadataPath);
			long currentVersion = metadata == null ? 0 : codec.version(metadata.value);
			Long expected = request.getExpectedVersion();
			if (expected != null && (expected == 0 || expected != currentVersion)) {
				throw conflict(type, request.getTargetId(), expected, currentVersion);
			}
			if (metadata == null) {
				return result(request.getTargetId(), type, ChangeRecord.Op.DELETE, 0, rootRevision(metadataRoot));
			}
			try {
				Data content = get(contentPath);
				List<CuratorOp> operations = new ArrayList<>();
				if (content != null) {
					operations.add(client.transactionOp().delete().withVersion(content.stat.getVersion()).forPath(contentPath));
				}
				operations.add(client.transactionOp().delete().withVersion(metadata.stat.getVersion()).forPath(metadataPath));
				client.transaction().forOperations(operations);
				return result(request.getTargetId(), type, ChangeRecord.Op.DELETE,
						currentVersion, rootRevision(metadataRoot));
			}
			catch (Exception e) {
				if (!isConflict(e)) { throw storage("remove " + type.name().toLowerCase()
						+ "[" + request.getTargetId() + "]", e); }
				if (expected != null) { throw conflict(type, request.getTargetId(), expected, currentVersion(metadataPath)); }
			}
		}
		throw new RuleStorageException("ZooKeeper remove " + type.name().toLowerCase()
				+ "[" + request.getTargetId() + "] exceeded concurrent retry limit");
	}

	private void ensureRoots() {
		try {
			client.createContainers(paths.chainMetaRoot());
			client.createContainers(paths.chainContentRoot());
			client.createContainers(paths.scriptMetaRoot());
			client.createContainers(paths.scriptContentRoot());
		}
		catch (Exception e) { throw storage("create rule roots", e); }
	}

	private void ensureWithinLimit(ChangeRecord.TargetType type, String id, Encoded encoded) {
		int size = Math.max(encoded.metadata.length, encoded.content.length);
		if (size > MAX_ZNODE_BYTES) {
			throw new RuleValidationException("ZooKeeper " + type.name().toLowerCase() + "[" + id
					+ "] encoded size " + size + " bytes exceeds the " + MAX_ZNODE_BYTES
					+ " byte safety limit (ZooKeeper jute.maxbuffer defaults to 1MB)");
		}
	}

	private Data get(String path) {
		try {
			Stat stat = new Stat();
			return new Data(client.getData().storingStatIn(stat).forPath(path), stat);
		}
		catch (KeeperException.NoNodeException e) { return null; }
		catch (Exception e) { throw storage("read " + path, e); }
	}

	private long currentVersion(String metadataPath) {
		Data value = get(metadataPath);
		return value == null ? 0 : codec.version(value.value);
	}

	private long metadataRevision(List<CuratorTransactionResult> results, String metadataPath) {
		for (CuratorTransactionResult result : results) {
			if (metadataPath.equals(result.getForPath()) && result.getType() == OperationType.SET_DATA
					&& result.getResultStat() != null) {
				return result.getResultStat().getMzxid();
			}
		}
		Data value = get(metadataPath);
		if (value == null) { throw new RuleStorageException("ZooKeeper metadata disappeared after commit: " + metadataPath); }
		return value.stat.getMzxid();
	}

	private long rootRevision(String root) {
		try {
			Stat stat = client.checkExists().forPath(root);
			return stat == null ? 0 : Math.max(stat.getPzxid(), stat.getMzxid());
		}
		catch (Exception e) { throw storage("read root revision " + root, e); }
	}

	private boolean isConflict(Exception e) {
		Throwable current = e;
		while (current != null) {
			if (current instanceof KeeperException.BadVersionException
					|| current instanceof KeeperException.NodeExistsException
					|| current instanceof KeeperException.NoNodeException) { return true; }
			current = current.getCause();
		}
		return false;
	}

	private VersionConflictException conflict(ChangeRecord.TargetType type, String id, long expected, long current) {
		return new VersionConflictException(type.name().toLowerCase() + "[" + id
				+ "] expected version[" + expected + "] but current version is[" + current + "]");
	}

	private RuleStorageException storage(String operation, Exception e) {
		if (e instanceof RuleStorageException) { return (RuleStorageException) e; }
		return new RuleStorageException("ZooKeeper " + operation + " failed: " + e.getMessage(), e);
	}

	private PublishResult result(String id, ChangeRecord.TargetType type, ChangeRecord.Op operation,
			long version, long sequence) {
		return PublishResult.builder().targetId(id).targetType(type).operation(operation)
				.version(version).sequence(sequence).build();
	}

	private interface Encoder { Encoded encode(long version); }
	private static final class Encoded {
		private final byte[] metadata;
		private final byte[] content;
		private Encoded(byte[] metadata, byte[] content) { this.metadata = metadata; this.content = content; }
	}
	private static final class Data {
		private final byte[] value;
		private final Stat stat;
		private Data(byte[] value, Stat stat) { this.value = value; this.stat = stat; }
	}
}
