package com.yomahub.liteflow.repository.etcd;

import cn.hutool.crypto.SecureUtil;
import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.PublishResult;
import com.yomahub.liteflow.publisher.PublishScriptRequest;
import com.yomahub.liteflow.publisher.RemoveRuleRequest;
import com.yomahub.liteflow.publisher.RulePublisher;
import com.yomahub.liteflow.publisher.exception.RuleStorageException;
import com.yomahub.liteflow.publisher.exception.VersionConflictException;
import com.yomahub.liteflow.repository.vo.ChainRecord;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import com.yomahub.liteflow.repository.vo.ScriptRecord;

final class EtcdRulePublisher implements RulePublisher {

	private static final int MAX_UNCONDITIONAL_RETRIES = 8;

	private final EtcdConnectionManager connection;
	private final EtcdKvFacade kv;
	private final EtcdKeys keys;
	private final EtcdRecordCodec codec;

	EtcdRulePublisher(EtcdPublisherConfig config) {
		this.connection = new EtcdConnectionManager(config);
		try {
			this.kv = new JetcdKvFacade(connection.client().getKVClient());
			this.keys = new EtcdKeys(config.getRootPath(), config.applicationName());
			this.codec = new EtcdRecordCodec();
		}
		catch (RuntimeException | Error e) {
			try {
				connection.close();
			}
			catch (RuntimeException closeError) {
				e.addSuppressed(closeError);
			}
			throw e;
		}
	}

	EtcdRulePublisher(EtcdKvFacade kv, EtcdKeys keys, EtcdRecordCodec codec) {
		this.connection = null;
		this.kv = kv;
		this.keys = keys;
		this.codec = codec;
	}

	@Override
	public PublishResult publishChain(PublishChainRequest request) {
		return publish(request.getChainId(), ChangeRecord.TargetType.CHAIN,
				request.getExpectedVersion(), keys.chainMeta(request.getChainId()),
				keys.chainContent(request.getChainId()), version -> {
					ChainRecord record = new ChainRecord();
					record.setChainId(request.getChainId());
					record.setEl(request.getEl());
					record.setRoute(request.getRoute());
					record.setNamespace(request.getNamespace());
					record.setVersion(version);
					record.setMd5(SecureUtil.md5(request.getEl()));
					record.setEnable(true);
					return new Encoded(codec.encodeChainMeta(record),
							codec.encodeContent(version, request.getEl()));
				});
	}

	@Override
	public PublishResult publishScript(PublishScriptRequest request) {
		return publish(request.getNodeId(), ChangeRecord.TargetType.SCRIPT,
				request.getExpectedVersion(), keys.scriptMeta(request.getNodeId()),
				keys.scriptContent(request.getNodeId()), version -> {
					ScriptRecord record = new ScriptRecord();
					record.setNodeId(request.getNodeId());
					record.setScript(request.getScript());
					record.setName(request.getName());
					record.setType(request.getType());
					record.setLanguage(request.getLanguage());
					record.setVersion(version);
					record.setMd5(SecureUtil.md5(request.getScript()));
					record.setEnable(true);
					return new Encoded(codec.encodeScriptMeta(record),
							codec.encodeContent(version, request.getScript()));
				});
	}

	@Override
	public PublishResult removeChain(RemoveRuleRequest request) {
		return remove(request, ChangeRecord.TargetType.CHAIN,
				keys.chainMeta(request.getTargetId()), keys.chainContent(request.getTargetId()));
	}

	@Override
	public PublishResult removeScript(RemoveRuleRequest request) {
		return remove(request, ChangeRecord.TargetType.SCRIPT,
				keys.scriptMeta(request.getTargetId()), keys.scriptContent(request.getTargetId()));
	}

	@Override
	public void close() {
		if (connection != null) {
			connection.close();
		}
	}

	private PublishResult publish(String targetId, ChangeRecord.TargetType targetType, Long expected,
			String metadataKey, String contentKey, Encoder encoder) {
		for (int attempt = 0; attempt < MAX_UNCONDITIONAL_RETRIES; attempt++) {
			EtcdKvFacade.Value current = kv.get(metadataKey);
			long currentVersion = current.exists() ? codec.version(current.value()) : 0;
			validateExpected(targetType, targetId, expected, currentVersion);
			long nextVersion = currentVersion + 1;
			Encoded encoded = encoder.encode(nextVersion);
			EtcdKvFacade.TxnResult result = kv.putPair(metadataKey, current.modRevision(),
					contentKey, encoded.content, encoded.metadata);
			if (result.succeeded()) {
				return result(targetId, targetType, ChangeRecord.Op.UPSERT, nextVersion, result.revision());
			}
			if (expected != null) {
				throw conflict(targetType, targetId, expected, currentVersion(metadataKey));
			}
		}
		throw new RuleStorageException("etcd publish " + targetType.name().toLowerCase()
				+ "[" + targetId + "] exceeded concurrent retry limit");
	}

	private PublishResult remove(RemoveRuleRequest request, ChangeRecord.TargetType targetType,
			String metadataKey, String contentKey) {
		for (int attempt = 0; attempt < MAX_UNCONDITIONAL_RETRIES; attempt++) {
			EtcdKvFacade.Value current = kv.get(metadataKey);
			long currentVersion = current.exists() ? codec.version(current.value()) : 0;
			Long expected = request.getExpectedVersion();
			if (expected != null && (expected == 0 || expected != currentVersion)) {
				throw conflict(targetType, request.getTargetId(), expected, currentVersion);
			}
			if (!current.exists()) {
				return result(request.getTargetId(), targetType, ChangeRecord.Op.DELETE,
						0, current.headerRevision());
			}
			EtcdKvFacade.TxnResult result = kv.deletePair(metadataKey, current.modRevision(), contentKey);
			if (result.succeeded()) {
				return result(request.getTargetId(), targetType, ChangeRecord.Op.DELETE,
						currentVersion, result.revision());
			}
			if (expected != null) {
				throw conflict(targetType, request.getTargetId(), expected, currentVersion(metadataKey));
			}
		}
		throw new RuleStorageException("etcd remove " + targetType.name().toLowerCase()
				+ "[" + request.getTargetId() + "] exceeded concurrent retry limit");
	}

	private void validateExpected(ChangeRecord.TargetType type, String id, Long expected, long current) {
		if (expected != null && expected != current) {
			throw conflict(type, id, expected, current);
		}
	}

	private long currentVersion(String metadataKey) {
		EtcdKvFacade.Value latest = kv.get(metadataKey);
		return latest.exists() ? codec.version(latest.value()) : 0;
	}

	private VersionConflictException conflict(ChangeRecord.TargetType type, String id,
			long expected, long current) {
		return new VersionConflictException(type.name().toLowerCase() + "[" + id
				+ "] expected version[" + expected + "] but current version is[" + current + "]");
	}

	private PublishResult result(String id, ChangeRecord.TargetType type, ChangeRecord.Op operation,
			long version, long sequence) {
		return PublishResult.builder().targetId(id).targetType(type).operation(operation)
				.version(version).sequence(sequence).build();
	}

	private interface Encoder {
		Encoded encode(long version);
	}

	private static final class Encoded {
		private final String metadata;
		private final String content;
		private Encoded(String metadata, String content) {
			this.metadata = metadata;
			this.content = content;
		}
	}
}
