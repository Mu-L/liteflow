package com.yomahub.liteflow.repository;

import com.yomahub.liteflow.repository.vo.ChainRecord;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import com.yomahub.liteflow.repository.vo.RuleManifest;
import com.yomahub.liteflow.repository.vo.ScriptRecord;

import java.util.List;

/**
 * Rule-DB 模式的规则权威源 SPI。
 * 实现类通过 ServiceLoader 注册（META-INF/services/com.yomahub.liteflow.repository.RuleRepository），
 * 必须提供无参构造器，连接等初始化在首次方法调用时基于 LiteflowConfigGetter.get().getRuleDb() 懒执行。
 *
 * @author Bryan.Zhang
 * @since 2.16.2
 */
public interface RuleRepository {

	/** 清单：全部 chain/script 的 id+version+md5+脚本元数据（不含内容） */
	RuleManifest fetchManifest();

	/** 按 id 取 chain 内容；不存在返回 null */
	ChainRecord fetchChain(String chainId);

	/** 按 id 取脚本内容；不存在返回 null */
	ScriptRecord fetchScript(String nodeId);

	/** 当前最大变更序号；无变更记录时返回 0 */
	long fetchLatestSeq();

	/** 取 seq 之后的增量变更（升序）；发现 seq 已断档（变更日志被清理）时抛 SeqGapException */
	List<ChangeRecord> fetchChangesSince(long seq);

	/** 可选推送通道（Redis 实现，SQL 空实现） */
	default void subscribe(RuleChangeListener listener) {
	}

	/** 释放连接资源 */
	default void close() {
	}

}
