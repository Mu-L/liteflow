package com.yomahub.liteflow.repository.etcd;

import com.yomahub.liteflow.publisher.exception.RuleStorageException;
import com.yomahub.liteflow.repository.RuleRepository;
import com.yomahub.liteflow.repository.vo.ChainMeta;
import com.yomahub.liteflow.repository.vo.ChainRecord;
import com.yomahub.liteflow.repository.vo.RuleManifest;
import com.yomahub.liteflow.repository.vo.ScriptMeta;
import com.yomahub.liteflow.repository.vo.ScriptRecord;

import java.util.ArrayList;
import java.util.List;

public final class EtcdRuleRepository implements RuleRepository {

	private final EtcdKvFacade kv;
	private final EtcdKeys keys;
	private final EtcdRecordCodec codec;

	EtcdRuleRepository(EtcdKvFacade kv, EtcdKeys keys, EtcdRecordCodec codec) {
		this.kv = kv;
		this.keys = keys;
		this.codec = codec;
	}

	@Override
	public RuleManifest fetchManifest() {
		EtcdKvFacade.Range chains = kv.range(keys.chainMetaPrefix());
		EtcdKvFacade.Range scripts = kv.range(keys.scriptMetaPrefix());
		List<ChainMeta> chainMetas = new ArrayList<>();
		for (EtcdKvFacade.Entry entry : chains.entries()) {
			if (codec.enabled(entry.value())) {
				chainMetas.add(codec.decodeChainMeta(keys.idFrom(keys.chainMetaPrefix(), entry.key()), entry.value()));
			}
		}
		List<ScriptMeta> scriptMetas = new ArrayList<>();
		for (EtcdKvFacade.Entry entry : scripts.entries()) {
			if (codec.enabled(entry.value())) {
				scriptMetas.add(codec.decodeScriptMeta(keys.idFrom(keys.scriptMetaPrefix(), entry.key()), entry.value()));
			}
		}
		RuleManifest manifest = new RuleManifest();
		manifest.setChains(chainMetas);
		manifest.setScripts(scriptMetas);
		manifest.setLatestSeq(Math.max(chains.revision(), scripts.revision()));
		return manifest;
	}

	@Override
	public ChainRecord fetchChain(String chainId) {
		EtcdKvFacade.Value first = kv.get(keys.chainMeta(chainId));
		if (!first.exists() || !codec.enabled(first.value())) {
			return null;
		}
		EtcdKvFacade.Value content = kv.get(keys.chainContent(chainId));
		EtcdKvFacade.Value second = kv.get(keys.chainMeta(chainId));
		ensureStable("chain", chainId, first, second);
		if (!content.exists()) {
			throw missing("chain", chainId);
		}
		return codec.decodeChain(chainId, second.value(), content.value());
	}

	@Override
	public ChainMeta fetchChainMeta(String chainId) {
		EtcdKvFacade.Value value = kv.get(keys.chainMeta(chainId));
		return !value.exists() || !codec.enabled(value.value())
				? null : codec.decodeChainMeta(chainId, value.value());
	}

	@Override
	public ScriptRecord fetchScript(String nodeId) {
		EtcdKvFacade.Value first = kv.get(keys.scriptMeta(nodeId));
		if (!first.exists() || !codec.enabled(first.value())) {
			return null;
		}
		EtcdKvFacade.Value content = kv.get(keys.scriptContent(nodeId));
		EtcdKvFacade.Value second = kv.get(keys.scriptMeta(nodeId));
		ensureStable("script", nodeId, first, second);
		if (!content.exists()) {
			throw missing("script", nodeId);
		}
		return codec.decodeScript(nodeId, second.value(), content.value());
	}

	@Override
	public ScriptMeta fetchScriptMeta(String nodeId) {
		EtcdKvFacade.Value value = kv.get(keys.scriptMeta(nodeId));
		return !value.exists() || !codec.enabled(value.value())
				? null : codec.decodeScriptMeta(nodeId, value.value());
	}

	private void ensureStable(String type, String id, EtcdKvFacade.Value first, EtcdKvFacade.Value second) {
		if (!second.exists() || first.modRevision() != second.modRevision()
				|| codec.version(first.value()) != codec.version(second.value())) {
			throw new RuleStorageException("etcd " + type + "[" + id + "] changed while reading content");
		}
	}

	private RuleStorageException missing(String type, String id) {
		return new RuleStorageException("etcd " + type + "[" + id + "] content is missing");
	}
}
