package com.yomahub.liteflow.repository.nacos;

import com.yomahub.liteflow.repository.RuleRepository;
import com.yomahub.liteflow.repository.vo.ChainMeta;
import com.yomahub.liteflow.repository.vo.ChainRecord;
import com.yomahub.liteflow.repository.vo.RuleManifest;
import com.yomahub.liteflow.repository.vo.ScriptMeta;
import com.yomahub.liteflow.repository.vo.ScriptRecord;

/** Repository backed by one atomically versioned Nacos catalog document. */
public final class NacosRuleRepository implements RuleRepository {

	private final NacosCatalogStore store;

	NacosRuleRepository(NacosCatalogStore store) {
		this.store = store;
	}

	@Override
	public RuleManifest fetchManifest() {
		return store.refresh().manifest();
	}

	@Override
	public ChainRecord fetchChain(String chainId) {
		return store.refresh().chain(chainId);
	}

	@Override
	public ChainMeta fetchChainMeta(String chainId) {
		return store.refresh().chainMeta(chainId);
	}

	@Override
	public ScriptRecord fetchScript(String nodeId) {
		return store.refresh().script(nodeId);
	}

	@Override
	public ScriptMeta fetchScriptMeta(String nodeId) {
		return store.refresh().scriptMeta(nodeId);
	}
}
