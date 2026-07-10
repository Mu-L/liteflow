package com.yomahub.liteflow.repository;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.crypto.digest.MD5;
import com.yomahub.liteflow.builder.el.LiteFlowChainELBuilder;
import com.yomahub.liteflow.enums.NodeTypeEnum;
import com.yomahub.liteflow.exception.ChainLoadException;
import com.yomahub.liteflow.flow.FlowBus;
import com.yomahub.liteflow.flow.element.Chain;
import com.yomahub.liteflow.flow.element.Node;
import com.yomahub.liteflow.log.LFLog;
import com.yomahub.liteflow.log.LFLoggerManager;
import com.yomahub.liteflow.meta.LiteflowMetaOperator;
import com.yomahub.liteflow.property.LiteflowConfig;
import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.repository.vo.ChainMeta;
import com.yomahub.liteflow.repository.vo.ChainRecord;
import com.yomahub.liteflow.repository.vo.RuleManifest;
import com.yomahub.liteflow.repository.vo.ScriptMeta;
import com.yomahub.liteflow.repository.vo.ScriptRecord;
import com.yomahub.liteflow.util.ElRegexUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Rule-DB 模式运行时：常驻版本戳索引 + 懒回源。后台变更同步/对账在 Task 5 补入，
 * 有界缓存在 Task 4 补入。
 *
 * @author Bryan.Zhang
 * @since 2.16.2
 */
public class RuleDbRuntime {

	private static final LFLog LOG = LFLoggerManager.getLogger(RuleDbRuntime.class);

	/** chainId -> 权威版本戳（常驻索引） */
	private static final Map<String, Long> CHAIN_VERSION_INDEX = new ConcurrentHashMap<>();

	/** nodeId -> 权威版本戳（常驻索引） */
	private static final Map<String, Long> SCRIPT_VERSION_INDEX = new ConcurrentHashMap<>();

	/** 已回源编译进 FlowBus 的 chain 当前版本（缓存态；Task 4 起由 RuleDbCache 协同） */
	private static final Map<String, Long> CHAIN_CACHED_VERSION = new ConcurrentHashMap<>();

	private static final Map<String, Long> SCRIPT_CACHED_VERSION = new ConcurrentHashMap<>();

	static final AtomicLong LAST_APPLIED_SEQ = new AtomicLong(0);

	private static volatile boolean initialized = false;

	public static boolean isActive() {
		if (!RuleRepositoryHolder.hasImplementation()) {
			return false;
		}
		LiteflowConfig config = LiteflowConfigGetter.get();
		RuleDbConfig ruleDb = config.getRuleDb();
		// 未显式配置 ruleDb 但 classpath 有实现，也视为激活（零配置理念）
		return ruleDb == null || ruleDb.getEnabled() == null || Boolean.TRUE.equals(ruleDb.getEnabled());
	}

	public static synchronized void init() {
		if (initialized) {
			return;
		}
		RuleRepository repo = RuleRepositoryHolder.get();
		RuleManifest manifest = repo.fetchManifest();

		// 注册 chain 影子
		if (CollUtil.isNotEmpty(manifest.getChains())) {
			for (ChainMeta cm : manifest.getChains()) {
				CHAIN_VERSION_INDEX.put(cm.getChainId(), cm.getVersion());
				FlowBus.addChain(cm.getChainId()); // 影子 Chain：只有 id，isCompiled=false
			}
		}
		// 注册 script 影子
		if (CollUtil.isNotEmpty(manifest.getScripts())) {
			for (ScriptMeta sm : manifest.getScripts()) {
				SCRIPT_VERSION_INDEX.put(sm.getNodeId(), sm.getVersion());
				registerShadowScript(sm);
			}
		}
		LAST_APPLIED_SEQ.set(manifest.getLatestSeq());
		initialized = true;

		// 初始化有界缓存（容量按 chain 条数）
		int capacity = 500;
		RuleDbConfig cacheCfg = LiteflowConfigGetter.get().getRuleDb();
		if (cacheCfg != null && cacheCfg.getCacheCapacity() != null) {
			capacity = cacheCfg.getCacheCapacity();
		}
		RuleDbCache.init(capacity);

		// 预热
		preload();
	}

	private static void preload() {
		RuleDbConfig ruleDb = LiteflowConfigGetter.get().getRuleDb();
		if (ruleDb == null || StrUtil.isBlank(ruleDb.getPreloadChainIds())) {
			return;
		}
		for (String chainId : ruleDb.getPreloadChainIds().split(",")) {
			String trimmed = chainId.trim();
			if (StrUtil.isNotBlank(trimmed) && CHAIN_VERSION_INDEX.containsKey(trimmed)) {
				try {
					ensureChainLoaded(trimmed);
					LiteFlowChainELBuilder.buildUnCompileChain(FlowBus.getChain(trimmed));
				} catch (Exception e) {
					LOG.warn("preload chain[{}] failed: {}", trimmed, e.getMessage());
				}
			}
		}
	}

	/**
	 * 注册脚本影子 Node：有元数据（type/language/name），无脚本源码，isCompiled=false。
	 * 直接 new Node 并放入 nodeMap，进入未编译态。
	 */
	private static void registerShadowScript(ScriptMeta sm) {
		NodeTypeEnum type = NodeTypeEnum.getEnumByCode(sm.getType());
		Node node = new Node(sm.getNodeId(), sm.getName(), type, null, sm.getLanguage());
		node.setCompiled(false);
		FlowBus.getNodeMap().put(sm.getNodeId(), node);
	}

	/** buildUnCompileChain 回源钩子：影子或版本失效时，拉内容填 EL，交由后续既有编译逻辑 */
	public static void ensureChainLoaded(String chainId) {
		Long authVersion = CHAIN_VERSION_INDEX.get(chainId);
		if (authVersion == null) {
			return; // 非 rule-db 管理的 chain（如手动 build），不干预
		}
		Chain chain = FlowBus.getChain(chainId);
		Long cachedVersion = CHAIN_CACHED_VERSION.get(chainId);
		if (chain != null && StrUtil.isNotBlank(chain.getEl())
				&& authVersion.equals(cachedVersion)) {
			return; // EL 已在手且版本一致
		}
		ChainRecord record = fetchChainWithRetry(chainId);
		if (record == null || !record.isEnable()) {
			throw new ChainLoadException(StrUtil.format("chain[{}] not found or disabled in rule repository", chainId));
		}
		if (chain == null) {
			// 索引里有但 FlowBus 无影子（理论上不发生，防御性补注册）
			FlowBus.addChain(chainId);
			chain = FlowBus.getChain(chainId);
		}
		chain.setEl(record.getEl());
		// 与 LiteFlowChainELBuilder.setEL 保持一致地计算并写入 elMd5，
		// 否则 FlowBus.addChain 的 elMd5Map.put 会因 null value 抛 NPE
		chain.setElMd5(MD5.create().digestHex(ElRegexUtil.normalize(record.getEl())));
		chain.setRouteEl(record.getRoute());
		if (StrUtil.isNotBlank(record.getNamespace())) {
			chain.setNamespace(record.getNamespace());
		}
		chain.setCompiled(false);
		CHAIN_CACHED_VERSION.put(chainId, record.getVersion());
		CHAIN_VERSION_INDEX.put(chainId, record.getVersion());
	}

	/**
	 * compileScriptNode 回源钩子：脚本影子/失效时拉源码填入 node。
	 * 注意：EL 编译期 {@code OperatorHelper.convert} 会对 Node 做 clone，
	 * 故 compileScriptNode 收到的 node 往往是 nodeMap 中影子的副本——
	 * 必须把源码写到传入的 node 上，而非重新 FlowBus.getNode(nodeId)。
	 */
	public static void ensureScriptLoaded(Node node) {
		String nodeId = node.getId();
		Long authVersion = SCRIPT_VERSION_INDEX.get(nodeId);
		if (authVersion == null) {
			return;
		}
		Long cachedVersion = SCRIPT_CACHED_VERSION.get(nodeId);
		if (StrUtil.isNotBlank(node.getScript()) && authVersion.equals(cachedVersion)) {
			return;
		}
		ScriptRecord record = fetchScriptWithRetry(nodeId);
		if (record == null || !record.isEnable()) {
			throw new ChainLoadException(StrUtil.format("script node[{}] not found or disabled in rule repository", nodeId));
		}
		node.setScript(record.getScript());
		node.setLanguage(record.getLanguage());
		SCRIPT_CACHED_VERSION.put(nodeId, record.getVersion());
		SCRIPT_VERSION_INDEX.put(nodeId, record.getVersion());
	}

	private static ChainRecord fetchChainWithRetry(String chainId) {
		int retry = retryTimes();
		RuntimeException last = null;
		for (int i = 0; i <= retry; i++) {
			try {
				return RuleRepositoryHolder.get().fetchChain(chainId);
			} catch (RuntimeException e) {
				last = e;
			}
		}
		throw new ChainLoadException(StrUtil.format("fetch chain[{}] failed after {} retries: {}",
				chainId, retry, last == null ? StrUtil.EMPTY : last.getMessage()));
	}

	private static ScriptRecord fetchScriptWithRetry(String nodeId) {
		int retry = retryTimes();
		RuntimeException last = null;
		for (int i = 0; i <= retry; i++) {
			try {
				return RuleRepositoryHolder.get().fetchScript(nodeId);
			} catch (RuntimeException e) {
				last = e;
			}
		}
		throw new ChainLoadException(StrUtil.format("fetch script[{}] failed after {} retries: {}",
				nodeId, retry, last == null ? StrUtil.EMPTY : last.getMessage()));
	}

	private static int retryTimes() {
		RuleDbConfig ruleDb = LiteflowConfigGetter.get().getRuleDb();
		return ruleDb == null || ruleDb.getFetchRetryTimes() == null ? 3 : ruleDb.getFetchRetryTimes();
	}

	/** 编译完成后登记：chain 驻留缓存 + 收集引用的脚本节点（引用计数+1） */
	public static void recordCompiledChain(String chainId) {
		List<String> scriptRefs = new ArrayList<>();
		try {
			for (Node n : LiteflowMetaOperator.getNodes(chainId)) {
				if (n.getType() != null && n.getType().isScript() && SCRIPT_VERSION_INDEX.containsKey(n.getId())) {
					scriptRefs.add(n.getId());
				}
			}
		} catch (Exception ignored) {
		}
		RuleDbCache.recordChainAccess(chainId, scriptRefs);
	}

	// ---- 索引/缓存态操作，供 Task 4/5 的 Cache/SyncManager 使用 ----

	public static Long getChainVersion(String chainId) {
		return CHAIN_VERSION_INDEX.get(chainId);
	}

	public static void putChainVersion(String chainId, long version) {
		CHAIN_VERSION_INDEX.put(chainId, version);
	}

	public static void putScriptVersion(String nodeId, long version) {
		SCRIPT_VERSION_INDEX.put(nodeId, version);
	}

	public static Map<String, Long> chainVersionIndex() {
		return CHAIN_VERSION_INDEX;
	}

	public static Map<String, Long> scriptVersionIndex() {
		return SCRIPT_VERSION_INDEX;
	}

	static void onChainEvicted(String chainId) {
		CHAIN_CACHED_VERSION.remove(chainId);
	}

	static void onScriptEvicted(String nodeId) {
		SCRIPT_CACHED_VERSION.remove(nodeId);
	}

	/** Task 5 的 applyChange/reconcile 使用的影子注册（清单新增 chain 时） */
	static void registerShadowChain(String chainId) {
		CHAIN_VERSION_INDEX.put(chainId, 0L);
		FlowBus.addChain(chainId);
	}

	/** 缓存态失效（Task 5 分级刷新用） */
	static void invalidateChainCache(String chainId) {
		CHAIN_CACHED_VERSION.remove(chainId);
		Chain chain = FlowBus.getChain(chainId);
		if (chain != null) {
			chain.setCompiled(false);
			chain.setConditionList(null);
			chain.setEl(null);
		}
	}

	static void invalidateScriptCache(String nodeId) {
		SCRIPT_CACHED_VERSION.remove(nodeId);
		Node node = FlowBus.getNode(nodeId);
		if (node != null) {
			node.setScript(null);
			node.setCompiled(false);
		}
	}

	public static synchronized void destroy() {
		RuleDbCache.destroy();
		CHAIN_VERSION_INDEX.clear();
		SCRIPT_VERSION_INDEX.clear();
		CHAIN_CACHED_VERSION.clear();
		SCRIPT_CACHED_VERSION.clear();
		LAST_APPLIED_SEQ.set(0);
		initialized = false;
		RuleRepository repo = RuleRepositoryHolder.get();
		if (repo != null) {
			try {
				repo.close();
			} catch (Exception ignored) {
			}
		}
	}
}
