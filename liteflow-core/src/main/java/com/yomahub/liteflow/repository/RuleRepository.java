package com.yomahub.liteflow.repository;

import com.yomahub.liteflow.repository.vo.ChainRecord;
import com.yomahub.liteflow.repository.vo.ChainMeta;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import com.yomahub.liteflow.repository.vo.RuleManifest;
import com.yomahub.liteflow.repository.vo.ScriptMeta;
import com.yomahub.liteflow.repository.vo.ScriptRecord;

/**
 * Backend-neutral authoritative rule reader owned by a {@link RuleDbProvider}.
 *
 * @author Bryan.Zhang
 * @since 2.16.1
 */
public interface RuleRepository {

	/** 清单：全部 chain/script 的 id+version+md5+脚本元数据（不含内容） */
	RuleManifest fetchManifest();

	/** 按 id 取 chain 内容；不存在返回 null */
	ChainRecord fetchChain(String chainId);

	/** 按 id 取 chain 元数据；不读取 EL 正文，不存在或停用返回 null */
	default ChainMeta fetchChainMeta(String chainId) {
		return null;
	}

	/** 按 id 取脚本内容；不存在返回 null */
	ScriptRecord fetchScript(String nodeId);

	/** 按 id 取脚本元数据；不读取脚本正文，不存在或停用返回 null */
	default ScriptMeta fetchScriptMeta(String nodeId) {
		return null;
	}

}
