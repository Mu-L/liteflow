package com.yomahub.liteflow.repository;

import cn.hutool.core.util.ObjectUtil;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.yomahub.liteflow.flow.FlowBus;
import com.yomahub.liteflow.flow.element.Chain;
import com.yomahub.liteflow.flow.element.Node;
import com.yomahub.liteflow.log.LFLog;
import com.yomahub.liteflow.log.LFLoggerManager;
import com.yomahub.liteflow.script.ScriptExecutorFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Rule-DB 有界缓存：容量按 chain 条数；淘汰 chain 时退影子并对其引用脚本引用计数-1，
 * 脚本引用计数归零则 unLoad 并退脚本影子。
 *
 * @author Bryan.Zhang
 * @since 2.16.2
 */
public class RuleDbCache {

	private static final LFLog LOG = LFLoggerManager.getLogger(RuleDbCache.class);

	private static volatile Cache<String, Boolean> chainCache;

	/** chainId -> 它引用的脚本 nodeId 列表 */
	private static final Map<String, List<String>> CHAIN_SCRIPT_REFS = new ConcurrentHashMap<>();

	/** nodeId -> 引用计数 */
	private static final Map<String, AtomicInteger> SCRIPT_REF_COUNT = new ConcurrentHashMap<>();

	public static synchronized void init(int capacity) {
		chainCache = Caffeine.newBuilder()
				.maximumSize(capacity)
				.<String, Boolean>evictionListener((chainId, v, cause) -> onChainEvicted(chainId))
				.build();
		CHAIN_SCRIPT_REFS.clear();
		SCRIPT_REF_COUNT.clear();
	}

	public static void recordChainAccess(String chainId, List<String> scriptNodeIds) {
		if (chainCache == null) {
			return;
		}
		// 更新引用关系（先释放旧引用，再登记新引用，避免重复计数）
		releaseRefs(chainId);
		CHAIN_SCRIPT_REFS.put(chainId, scriptNodeIds);
		for (String nodeId : scriptNodeIds) {
			SCRIPT_REF_COUNT.computeIfAbsent(nodeId, k -> new AtomicInteger(0)).incrementAndGet();
		}
		chainCache.put(chainId, Boolean.TRUE);
	}

	private static void onChainEvicted(String chainId) {
		// 退 chain 影子
		Chain chain = FlowBus.getChain(chainId);
		if (ObjectUtil.isNotNull(chain)) {
			chain.setCompiled(false);
			chain.setConditionList(null);
			chain.setEl(null);
		}
		RuleDbRuntime.onChainEvicted(chainId);
		// 释放脚本引用
		releaseRefs(chainId);
	}

	private static void releaseRefs(String chainId) {
		List<String> refs = CHAIN_SCRIPT_REFS.remove(chainId);
		if (refs == null) {
			return;
		}
		for (String nodeId : refs) {
			AtomicInteger count = SCRIPT_REF_COUNT.get(nodeId);
			if (count != null && count.decrementAndGet() <= 0) {
				SCRIPT_REF_COUNT.remove(nodeId);
				unloadScript(nodeId);
			}
		}
	}

	private static void unloadScript(String nodeId) {
		Node node = FlowBus.getNode(nodeId);
		if (node == null || node.getType() == null || !node.getType().isScript()) {
			return;
		}
		try {
			ScriptExecutorFactory.loadInstance().getScriptExecutor(node.getLanguage()).unLoad(nodeId);
		} catch (Exception e) {
			LOG.warn("unload script[{}] failed: {}", nodeId, e.getMessage());
		}
		node.setScript(null);
		node.setCompiled(false);
		RuleDbRuntime.onScriptEvicted(nodeId);
	}

	public static int scriptRefCount(String nodeId) {
		AtomicInteger c = SCRIPT_REF_COUNT.get(nodeId);
		return c == null ? 0 : c.get();
	}

	/** 强制清理未决的淘汰任务（测试断言前调用以获得确定性） */
	public static void cleanUp() {
		if (chainCache != null) {
			chainCache.cleanUp();
		}
	}

	public static synchronized void destroy() {
		if (chainCache != null) {
			chainCache.invalidateAll();
			chainCache.cleanUp();
		}
		chainCache = null;
		CHAIN_SCRIPT_REFS.clear();
		SCRIPT_REF_COUNT.clear();
	}
}
