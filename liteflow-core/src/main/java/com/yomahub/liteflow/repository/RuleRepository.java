package com.yomahub.liteflow.repository;

import com.yomahub.liteflow.repository.vo.ChainRecord;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import com.yomahub.liteflow.repository.vo.RuleManifest;
import com.yomahub.liteflow.repository.vo.ScriptRecord;

import java.util.List;
import java.util.Collections;

/**
 * Rule-DB 模式的规则权威源 SPI。
 * 实现类通过 ServiceLoader 注册（META-INF/services/com.yomahub.liteflow.repository.RuleRepository），
 * 必须提供无参构造器，连接等初始化在首次方法调用时基于 LiteflowConfigGetter.get().getRuleDb() 懒执行。
 *
 * @author Bryan.Zhang
 * @since 2.16.1
 */
public interface RuleRepository {

	/** 清单：全部 chain/script 的 id+version+md5+脚本元数据（不含内容） */
	RuleManifest fetchManifest();

	/** 按 id 取 chain 内容；不存在返回 null */
	ChainRecord fetchChain(String chainId);

	/** 按 id 取脚本内容；不存在返回 null */
	ScriptRecord fetchScript(String nodeId);

	/** 当前最大变更序号；无变更记录时返回 0 */
	@Deprecated
	default long fetchLatestSeq() {
		return 0L;
	}

	/** 取 seq 之后的增量变更（升序）；发现 seq 已断档（变更日志被清理）时抛 SeqGapException */
	@Deprecated
	default List<ChangeRecord> fetchChangesSince(long seq) {
		return Collections.emptyList();
	}

	/** 可选推送通道（Redis 实现，SQL 空实现） */
	@Deprecated
	default void subscribe(RuleChangeListener listener) {
	}

	/**
	 * seq 轮询的插件级默认周期（秒），仅在 liteflow.rule-db.seq-poll-seconds 未配置时生效。
	 * 无推送通道的实现（SQL）轮询是唯一感知手段，默认激进（3s）；
	 * 有推送通道的实现（Redis）轮询只是丢消息兜底，应覆写为更宽松的值（30s）。
	 */
	@Deprecated
	default int defaultSeqPollSeconds() {
		return 3;
	}

	/** 释放连接资源 */
	default void close() {
	}

}
