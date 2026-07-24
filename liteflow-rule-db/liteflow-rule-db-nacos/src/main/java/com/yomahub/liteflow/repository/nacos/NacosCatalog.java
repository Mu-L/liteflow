package com.yomahub.liteflow.repository.nacos;

import com.yomahub.liteflow.repository.vo.ChainMeta;
import com.yomahub.liteflow.repository.vo.ChainRecord;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import com.yomahub.liteflow.repository.vo.RuleManifest;
import com.yomahub.liteflow.repository.vo.ScriptMeta;
import com.yomahub.liteflow.repository.vo.ScriptRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/** Immutable in-memory representation of one application's atomic Nacos catalog. */
final class NacosCatalog {

	private final long sequence;
	private final Map<String, ChainRecord> chains;
	private final Map<String, ScriptRecord> scripts;
	private final ChangeRecord lastChange;

	NacosCatalog(long sequence, Map<String, ChainRecord> chains,
			Map<String, ScriptRecord> scripts, ChangeRecord lastChange) {
		this.sequence = sequence;
		this.chains = copyChains(chains);
		this.scripts = copyScripts(scripts);
		this.lastChange = copy(lastChange);
	}

	static NacosCatalog empty() {
		return new NacosCatalog(0, Collections.emptyMap(), Collections.emptyMap(), null);
	}

	long sequence() {
		return sequence;
	}

	long chainVersion(String chainId) {
		ChainRecord record = chains.get(chainId);
		return record == null ? 0 : record.getVersion();
	}

	long scriptVersion(String nodeId) {
		ScriptRecord record = scripts.get(nodeId);
		return record == null ? 0 : record.getVersion();
	}

	ChainRecord chain(String chainId) {
		return copy(chains.get(chainId));
	}

	ScriptRecord script(String nodeId) {
		return copy(scripts.get(nodeId));
	}

	ChainMeta chainMeta(String chainId) {
		ChainRecord record = chains.get(chainId);
		return record == null || !record.isEnable() ? null
				: new ChainMeta(record.getChainId(), record.getVersion(), record.getMd5());
	}

	ScriptMeta scriptMeta(String nodeId) {
		ScriptRecord record = scripts.get(nodeId);
		return record == null || !record.isEnable() ? null
				: new ScriptMeta(record.getNodeId(), record.getVersion(), record.getMd5(),
						record.getType(), record.getLanguage(), record.getName());
	}

	RuleManifest manifest() {
		RuleManifest manifest = new RuleManifest();
		ArrayList<ChainMeta> chainMetas = new ArrayList<>();
		for (String id : chains.keySet()) {
			ChainMeta meta = chainMeta(id);
			if (meta != null) {
				chainMetas.add(meta);
			}
		}
		ArrayList<ScriptMeta> scriptMetas = new ArrayList<>();
		for (String id : scripts.keySet()) {
			ScriptMeta meta = scriptMeta(id);
			if (meta != null) {
				scriptMetas.add(meta);
			}
		}
		manifest.setChains(chainMetas);
		manifest.setScripts(scriptMetas);
		manifest.setLatestSeq(sequence);
		return manifest;
	}

	ChangeRecord lastChange() {
		return copy(lastChange);
	}

	NacosCatalog withChain(ChainRecord record, ChangeRecord change) {
		Map<String, ChainRecord> next = copyChains(chains);
		next.put(record.getChainId(), copy(record));
		return new NacosCatalog(change.getSeq(), next, scripts, change);
	}

	NacosCatalog withoutChain(String chainId, ChangeRecord change) {
		Map<String, ChainRecord> next = copyChains(chains);
		next.remove(chainId);
		return new NacosCatalog(change.getSeq(), next, scripts, change);
	}

	NacosCatalog withScript(ScriptRecord record, ChangeRecord change) {
		Map<String, ScriptRecord> next = copyScripts(scripts);
		next.put(record.getNodeId(), copy(record));
		return new NacosCatalog(change.getSeq(), chains, next, change);
	}

	NacosCatalog withoutScript(String nodeId, ChangeRecord change) {
		Map<String, ScriptRecord> next = copyScripts(scripts);
		next.remove(nodeId);
		return new NacosCatalog(change.getSeq(), chains, next, change);
	}

	Map<String, ChainRecord> chains() {
		return Collections.unmodifiableMap(chains);
	}

	Map<String, ScriptRecord> scripts() {
		return Collections.unmodifiableMap(scripts);
	}

	private static Map<String, ChainRecord> copyChains(Map<String, ChainRecord> source) {
		Map<String, ChainRecord> result = new TreeMap<>();
		if (source != null) {
			for (Map.Entry<String, ChainRecord> entry : source.entrySet()) {
				result.put(entry.getKey(), copy(entry.getValue()));
			}
		}
		return result;
	}

	private static Map<String, ScriptRecord> copyScripts(Map<String, ScriptRecord> source) {
		Map<String, ScriptRecord> result = new TreeMap<>();
		if (source != null) {
			for (Map.Entry<String, ScriptRecord> entry : source.entrySet()) {
				result.put(entry.getKey(), copy(entry.getValue()));
			}
		}
		return result;
	}

	private static ChainRecord copy(ChainRecord source) {
		if (source == null) { return null; }
		ChainRecord result = new ChainRecord();
		result.setChainId(source.getChainId());
		result.setEl(source.getEl());
		result.setRoute(source.getRoute());
		result.setNamespace(source.getNamespace());
		result.setVersion(source.getVersion());
		result.setMd5(source.getMd5());
		result.setEnable(source.isEnable());
		return result;
	}

	private static ScriptRecord copy(ScriptRecord source) {
		if (source == null) { return null; }
		ScriptRecord result = new ScriptRecord();
		result.setNodeId(source.getNodeId());
		result.setScript(source.getScript());
		result.setName(source.getName());
		result.setType(source.getType());
		result.setLanguage(source.getLanguage());
		result.setVersion(source.getVersion());
		result.setMd5(source.getMd5());
		result.setEnable(source.isEnable());
		return result;
	}

	private static ChangeRecord copy(ChangeRecord source) {
		return source == null ? null : new ChangeRecord(source.getSeq(), source.getTargetType(),
				source.getTargetId(), source.getOp(), source.getVersion());
	}
}
