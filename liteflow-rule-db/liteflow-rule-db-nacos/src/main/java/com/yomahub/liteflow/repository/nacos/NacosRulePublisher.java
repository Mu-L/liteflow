package com.yomahub.liteflow.repository.nacos;

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

/** Atomic Nacos publisher using catalog-level compare-and-set. */
final class NacosRulePublisher implements RulePublisher {

	private static final int MAX_CAS_RETRIES = 8;

	private final NacosConnectionManager connection;
	private final NacosConfigFacade facade;
	private final NacosConfigKey key;
	private final NacosCatalogCodec codec;
	private final long timeoutMillis;

	NacosRulePublisher(NacosPublisherConfig config) {
		this.connection = new NacosConnectionManager(config);
		try {
			this.facade = new ClientNacosConfigFacade(connection.client());
			this.key = new NacosConfigKey(config.getDataIdPrefix(), config.applicationName(), config.getGroup());
			this.codec = new NacosCatalogCodec();
			this.timeoutMillis = config.getTimeoutMillis();
		}
		catch (RuntimeException | Error e) {
			connection.close();
			throw e;
		}
	}

	NacosRulePublisher(NacosConfigFacade facade, NacosConfigKey key,
			NacosCatalogCodec codec, long timeoutMillis) {
		this.connection = null;
		this.facade = facade;
		this.key = key;
		this.codec = codec;
		this.timeoutMillis = timeoutMillis;
	}

	@Override
	public PublishResult publishChain(PublishChainRequest request) {
		return mutate(request.getChainId(), ChangeRecord.TargetType.CHAIN, ChangeRecord.Op.UPSERT,
				request.getExpectedVersion(), (catalog, version, change) -> {
					ChainRecord record = new ChainRecord();
					record.setChainId(request.getChainId());
					record.setEl(request.getEl());
					record.setRoute(request.getRoute());
					record.setNamespace(request.getNamespace());
					record.setVersion(version);
					record.setMd5(SecureUtil.md5(request.getEl()));
					record.setEnable(true);
					return catalog.withChain(record, change);
				});
	}

	@Override
	public PublishResult publishScript(PublishScriptRequest request) {
		return mutate(request.getNodeId(), ChangeRecord.TargetType.SCRIPT, ChangeRecord.Op.UPSERT,
				request.getExpectedVersion(), (catalog, version, change) -> {
					ScriptRecord record = new ScriptRecord();
					record.setNodeId(request.getNodeId());
					record.setScript(request.getScript());
					record.setName(request.getName());
					record.setType(request.getType());
					record.setLanguage(request.getLanguage());
					record.setVersion(version);
					record.setMd5(SecureUtil.md5(request.getScript()));
					record.setEnable(true);
					return catalog.withScript(record, change);
				});
	}

	@Override
	public PublishResult removeChain(RemoveRuleRequest request) {
		return mutate(request.getTargetId(), ChangeRecord.TargetType.CHAIN, ChangeRecord.Op.DELETE,
				request.getExpectedVersion(),
				(catalog, version, change) -> catalog.withoutChain(request.getTargetId(), change));
	}

	@Override
	public PublishResult removeScript(RemoveRuleRequest request) {
		return mutate(request.getTargetId(), ChangeRecord.TargetType.SCRIPT, ChangeRecord.Op.DELETE,
				request.getExpectedVersion(),
				(catalog, version, change) -> catalog.withoutScript(request.getTargetId(), change));
	}

	@Override
	public void close() {
		if (connection != null) {
			connection.close();
		}
	}

	private PublishResult mutate(String targetId, ChangeRecord.TargetType targetType,
			ChangeRecord.Op operation, Long expectedVersion, Mutation mutation) {
		for (int attempt = 0; attempt < MAX_CAS_RETRIES; attempt++) {
			String currentContent = facade.get(key.dataId(), key.group(), timeoutMillis);
			NacosCatalog current = codec.decode(currentContent);
			long currentVersion = version(current, targetType, targetId);
			validateExpected(targetType, operation, targetId, expectedVersion, currentVersion);
			long nextSequence = increment("catalog sequence", current.sequence());
			long committedVersion = operation == ChangeRecord.Op.UPSERT
					? increment(targetType.name().toLowerCase() + " version", currentVersion) : currentVersion;
			ChangeRecord change = new ChangeRecord(nextSequence, targetType, targetId,
					operation, committedVersion);
			NacosCatalog next = mutation.apply(current, committedVersion, change);
			if (facade.publishCas(key.dataId(), key.group(), codec.encode(next), codec.md5(currentContent))) {
				return PublishResult.builder().targetId(targetId).targetType(targetType).operation(operation)
						.version(committedVersion).sequence(nextSequence).build();
			}

			if (expectedVersion != null) {
				NacosCatalog latest = codec.decode(facade.get(key.dataId(), key.group(), timeoutMillis));
				long latestVersion = version(latest, targetType, targetId);
				if (latestVersion != currentVersion) {
					throw conflict(targetType, targetId, expectedVersion, latestVersion);
				}
			}
		}
		throw new RuleStorageException("Nacos " + operation.name().toLowerCase() + " "
				+ targetType.name().toLowerCase() + "[" + targetId + "] exceeded CAS retry limit");
	}

	private long version(NacosCatalog catalog, ChangeRecord.TargetType type, String id) {
		return type == ChangeRecord.TargetType.CHAIN ? catalog.chainVersion(id) : catalog.scriptVersion(id);
	}

	private void validateExpected(ChangeRecord.TargetType type, ChangeRecord.Op operation,
			String id, Long expected, long current) {
		if (expected == null) { return; }
		if (operation == ChangeRecord.Op.DELETE && expected == 0) {
			throw conflict(type, id, expected, current);
		}
		if (expected != current) {
			throw conflict(type, id, expected, current);
		}
	}

	private long increment(String field, long current) {
		if (current == Long.MAX_VALUE) {
			throw new RuleStorageException("Nacos " + field + " overflow");
		}
		return current + 1;
	}

	private VersionConflictException conflict(ChangeRecord.TargetType type, String id,
			long expected, long current) {
		return new VersionConflictException(type.name().toLowerCase() + "[" + id
				+ "] expected version[" + expected + "] but current version is[" + current + "]");
	}

	private interface Mutation {
		NacosCatalog apply(NacosCatalog catalog, long version, ChangeRecord change);
	}
}
