package com.yomahub.liteflow.repository.vo;

import java.util.ArrayList;
import java.util.List;

/**
 * Rule-DB 模式清单：全部 chain/script 的 id+version+md5+脚本元数据（不含内容）。
 * 由 {@code RuleRepository.fetchManifest()} 返回，供运行时做差异比对与对账。
 *
 * @author Bryan.Zhang
 * @since 2.16.1
 */
public class RuleManifest {

	private List<ChainMeta> chains = new ArrayList<>();

	private List<ScriptMeta> scripts = new ArrayList<>();

	private long latestSeq;

	public List<ChainMeta> getChains() {
		return chains;
	}

	public void setChains(List<ChainMeta> chains) {
		this.chains = chains;
	}

	public List<ScriptMeta> getScripts() {
		return scripts;
	}

	public void setScripts(List<ScriptMeta> scripts) {
		this.scripts = scripts;
	}

	public long getLatestSeq() {
		return latestSeq;
	}

	public void setLatestSeq(long latestSeq) {
		this.latestSeq = latestSeq;
	}

}
