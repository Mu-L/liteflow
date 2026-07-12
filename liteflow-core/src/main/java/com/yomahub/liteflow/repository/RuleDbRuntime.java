package com.yomahub.liteflow.repository;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.crypto.digest.MD5;
import com.yomahub.liteflow.builder.el.LiteFlowChainELBuilder;
import com.yomahub.liteflow.enums.NodeTypeEnum;
import com.yomahub.liteflow.exception.ChainLoadException;
import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.flow.FlowBus;
import com.yomahub.liteflow.flow.element.Chain;
import com.yomahub.liteflow.flow.element.Node;
import com.yomahub.liteflow.log.LFLog;
import com.yomahub.liteflow.log.LFLoggerManager;
import com.yomahub.liteflow.meta.LiteflowMetaOperator;
import com.yomahub.liteflow.property.LiteflowConfig;
import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import com.yomahub.liteflow.repository.vo.ChainMeta;
import com.yomahub.liteflow.repository.vo.ChainRecord;
import com.yomahub.liteflow.repository.vo.RuleManifest;
import com.yomahub.liteflow.repository.vo.ScriptMeta;
import com.yomahub.liteflow.repository.vo.ScriptRecord;
import com.yomahub.liteflow.util.ElRegexUtil;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Rule-DB 模式运行时：常驻版本戳索引 + 懒回源。后台变更同步/对账在 Task 5 补入，
 * 有界缓存在 Task 4 补入。
 *
 * @author Bryan.Zhang
 * @since 2.16.1
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

	/** 缓存态内容的 content_md5：对账时 version 相同再比 md5（双保险，spec §7），发现脏写则失效 */
	private static final Map<String, String> CHAIN_CACHED_MD5 = new ConcurrentHashMap<>();

	private static final Map<String, String> SCRIPT_CACHED_MD5 = new ConcurrentHashMap<>();

	/** Shadow objects registered by this runtime, used to avoid removing application-owned metadata. */
	private static final Map<String, Chain> SHADOW_CHAINS = new ConcurrentHashMap<>();
	private static final Map<String, Node> SHADOW_SCRIPTS = new ConcurrentHashMap<>();

	static final AtomicLong LAST_APPLIED_SEQ = new AtomicLong(0);

	private static volatile boolean initialized = false;
	private static volatile RuleRepository activeRepository;

	/**
	 * isActive() 结果缓存：该标志在运行期不会改变（SPI 实现是否在 classpath、enabled 配置均固定），
	 * 但每次 buildUnCompileChain/compileScriptNode 都会调用，而 RuleRepositoryHolder.hasImplementation()
	 * 内部的 get() 是 synchronized —— 缓存后彻底脱离 holder 监视器热路径。
	 * destroy() 置 null，下次 init 重算（也使非 rule-db 应用在 destroy 后不残留过期 true）。
	 */
	private static volatile Boolean activeFlag;

	public static boolean isActive() {
		Boolean cached = activeFlag;
		if (cached != null) {
			return cached;
		}
		if (!RuleRepositoryHolder.hasImplementation()) {
			activeFlag = Boolean.FALSE;
			return false;
		}
		LiteflowConfig config = LiteflowConfigGetter.get();
		RuleDbConfig ruleDb = config.getRuleDb();
		// 未显式配置 ruleDb 但 classpath 有实现，也视为激活（零配置理念）
		boolean result = ruleDb == null || ruleDb.getEnabled() == null || Boolean.TRUE.equals(ruleDb.getEnabled());
		activeFlag = result;
		return result;
	}

	public static synchronized void init() {
		if (initialized) {
			return;
		}
		// Resolve the legacy SPI first so a classpath containing both integration
		// styles fails deterministically instead of silently preferring a provider.
		RuleRepositoryHolder.get();
		RuleDbProvider provider = RuleDbProviderHolder.get();
		if (provider == null) {
			// Transitional RuleRepository implementations remain usable until their
			// unified provider adapters are introduced.
			RuleRepository legacyRepository = RuleRepositoryHolder.get();
			if (legacyRepository == null) {
				return;
			}
			provider = legacyProvider(legacyRepository);
		}

		try {
			// Open the source before reading the manifest so events observed during the
			// snapshot are buffered and replayed after its sequence baseline is known.
			RuleDbSyncManager.open(provider);
			activeRepository = RuleDbSyncManager.activeRepository();
			RuleManifest manifest = activeRepository.fetchManifest();
			validateManifest(manifest);
			validateManifestOwnership(manifest);

			// 注册 chain 影子
			if (CollUtil.isNotEmpty(manifest.getChains())) {
				for (ChainMeta cm : manifest.getChains()) {
					registerShadowChain(cm);
				}
			}
			// 注册 script 影子
			if (CollUtil.isNotEmpty(manifest.getScripts())) {
				for (ScriptMeta sm : manifest.getScripts()) {
					registerShadowScript(sm);
					if (SHADOW_SCRIPTS.containsKey(sm.getNodeId())) {
						SCRIPT_VERSION_INDEX.put(sm.getNodeId(), sm.getVersion());
					}
				}
			}
			initialized = true;

			// 初始化有界缓存（容量按 chain 条数）
			int capacity = 500;
			RuleDbConfig cacheCfg = LiteflowConfigGetter.get().getRuleDb();
			if (cacheCfg != null && cacheCfg.getCacheCapacity() != null) {
				capacity = cacheCfg.getCacheCapacity();
			}
			RuleDbCache.init(capacity);

			// Activate buffered changes at the manifest baseline, then reconcile and preload.
			RuleDbSyncManager.activate(manifest.getLatestSeq());
			RuleDbSyncManager.startReconcileScheduler();
			preload();
		} catch (RuntimeException e) {
			RuleDbSyncManager.stop();
			clearRuntimeState();
			throw e;
		}
	}

	private static RuleDbProvider legacyProvider(RuleRepository repository) {
		return new RuleDbProvider() {
			private final LegacyRuleChangeSource source = new LegacyRuleChangeSource(repository);

			@Override
			public RuleRepository repository() {
				return repository;
			}

			@Override
			public RuleChangeSource changeSource() {
				return source;
			}

			@Override
			public void close() {
				source.close();
				repository.close();
			}
		};
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
		if (FlowBus.containNode(sm.getNodeId())) {
			Node existing = FlowBus.getNode(sm.getNodeId());
			if (SHADOW_SCRIPTS.get(sm.getNodeId()) != existing) {
				throw collision("script", sm.getNodeId());
			}
		}
		NodeTypeEnum type = NodeTypeEnum.getEnumByCode(sm.getType());
		Node node = new Node(sm.getNodeId(), sm.getName(), type, null, sm.getLanguage());
		node.setCompiled(false);
		FlowBus.getNodeMap().put(sm.getNodeId(), node);
		SHADOW_SCRIPTS.put(sm.getNodeId(), node);
	}

	private static void registerShadowChain(ChainMeta cm) {
		String chainId = cm.getChainId();
		Chain existing = FlowBus.getChain(chainId);
		Chain owned = SHADOW_CHAINS.get(chainId);
		if (existing != null && existing != owned) {
			throw collision("chain", chainId);
		}
		if (existing == null) {
			FlowBus.addChain(chainId);
			existing = FlowBus.getChain(chainId);
		}
		if (existing != null) {
			SHADOW_CHAINS.put(chainId, existing);
			CHAIN_VERSION_INDEX.put(chainId, cm.getVersion());
		}
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
			if (chain != null) {
				SHADOW_CHAINS.put(chainId, chain);
			}
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
		if (StrUtil.isNotBlank(record.getMd5())) {
			CHAIN_CACHED_MD5.put(chainId, record.getMd5());
		}
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
		SHADOW_SCRIPTS.put(nodeId, node);
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
		if (StrUtil.isNotBlank(record.getMd5())) {
			SCRIPT_CACHED_MD5.put(nodeId, record.getMd5());
		}
	}

	private static ChainRecord fetchChainWithRetry(String chainId) {
		int retry = retryTimes();
		RuntimeException last = null;
		for (int i = 0; i <= retry; i++) {
			try {
				return repositoryForRead().fetchChain(chainId);
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
				return repositoryForRead().fetchScript(nodeId);
			} catch (RuntimeException e) {
				last = e;
			}
		}
		throw new ChainLoadException(StrUtil.format("fetch script[{}] failed after {} retries: {}",
				nodeId, retry, last == null ? StrUtil.EMPTY : last.getMessage()));
	}

	private static ScriptMeta fetchScriptMetaWithRetry(String nodeId) {
		int retry = retryTimes();
		RuntimeException last = null;
		for (int i = 0; i <= retry; i++) {
			try {
				return repositoryForRead().fetchScriptMeta(nodeId);
			} catch (RuntimeException e) {
				last = e;
			}
		}
		throw new ChainLoadException(StrUtil.format("fetch script metadata[{}] failed after {} retries: {}",
				nodeId, retry, last == null ? StrUtil.EMPTY : last.getMessage()));
	}

	private static int retryTimes() {
		RuleDbConfig ruleDb = LiteflowConfigGetter.get().getRuleDb();
		return ruleDb == null || ruleDb.getFetchRetryTimes() == null ? 3 : ruleDb.getFetchRetryTimes();
	}

	private static RuleRepository repositoryForRead() {
		RuleRepository repository = activeRepository;
		if (repository != null) {
			return repository;
		}
		RuleDbProvider provider = RuleDbProviderHolder.get();
		if (provider != null && provider.repository() != null) {
			return provider.repository();
		}
		return RuleRepositoryHolder.get();
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

	public static Map<String, Long> scriptVersionIndex() {
		return SCRIPT_VERSION_INDEX;
	}

	static void onChainEvicted(String chainId) {
		CHAIN_CACHED_VERSION.remove(chainId);
		CHAIN_CACHED_MD5.remove(chainId);
	}

	static void onScriptEvicted(String nodeId) {
		SCRIPT_CACHED_VERSION.remove(nodeId);
		SCRIPT_CACHED_MD5.remove(nodeId);
	}

	/** 缓存态失效（Task 5 分级刷新用） */
	static void invalidateChainCache(String chainId) {
		CHAIN_CACHED_VERSION.remove(chainId);
		CHAIN_CACHED_MD5.remove(chainId);
		Chain chain = FlowBus.getChain(chainId);
		if (chain != null) {
			chain.setCompiled(false);
			chain.setConditionList(null);
			chain.setEl(null);
		}
	}

	static void invalidateScriptCache(String nodeId) {
		SCRIPT_CACHED_VERSION.remove(nodeId);
		SCRIPT_CACHED_MD5.remove(nodeId);
		Node node = FlowBus.getNode(nodeId);
		if (node != null) {
			node.setScript(null);
			node.setCompiled(false);
		}
		// 已编译 chain 的条件树里持有的是克隆 Node（浅拷贝连 isCompiled/instance 一起拷），
		// 只失效 nodeMap 驻留的那一个时，其余克隆仍是已编译态，在别的 chain 重载执行器产物之前
		// 会一直跑旧脚本——必须把所有条件树里的同 id 克隆一并失效（对齐 FlowBus.reloadScript 的克隆更新语义）
		for (Chain chain : FlowBus.getChainMap().values()) {
			if (CollUtil.isEmpty(chain.getConditionList())) {
				continue; // 影子/已失效 chain 无条件树
			}
			List<Node> nodesInChain;
			try {
				nodesInChain = LiteflowMetaOperator.getNodes(chain);
			} catch (Exception e) {
				// 树中引用的子链可能刚被失效退影子（conditionList=null），递归收集会 NPE；
				// 跳过即可——该子链重编时会经 compileScriptNode 拿到新脚本
				continue;
			}
			for (Node n : nodesInChain) {
				if (nodeId.equals(n.getId())) {
					n.setScript(null);
					n.setCompiled(false);
				}
			}
		}
	}

	/**
	 * 单条变更处理（分级刷新 / 惰性失效）：
	 * - UPSERT chain：更新索引版本；新增则注册影子；复用 {@link #invalidateChainCache} 失效缓存态，下次执行懒加载新版。
	 * - UPSERT script：更新索引；复用 {@link #invalidateScriptCache} 清缓存态让下次 getInstance 回源重编。
	 * - DELETE：移除索引 + 缓存态 + FlowBus 中的条目。
	 * 由 {@link RuleDbSyncManager} 的轮询/订阅路径回调。
	 */
	public static void applyChange(ChangeRecord change) {
		String id = change.getTargetId();
		long version = change.getVersion();
		if (change.getTargetType() == ChangeRecord.TargetType.CHAIN) {
			assertNoForeignChain(id);
			if (change.getOp() == ChangeRecord.Op.DELETE) {
				CHAIN_VERSION_INDEX.remove(id);
				CHAIN_CACHED_VERSION.remove(id);
				CHAIN_CACHED_MD5.remove(id);
				removeOwnedChain(id);
			} else {
				Long cur = CHAIN_VERSION_INDEX.get(id);
				// spec §8.5 幂等：缓存版本 ≥ 通知版本则忽略（DELETE 分支不受此限）
				if (cur != null && version <= cur) {
					return;
				}
				if (cur == null) {
					// 新增 chain：注册影子
					registerShadowChain(new ChainMeta(id, version, null));
				}
				CHAIN_VERSION_INDEX.put(id, version);
				// 缓存态失效：置为过期，下次 ensureChainLoaded 回源
				invalidateChainCache(id);
			}
		} else {
			assertNoForeignScript(id);
			if (change.getOp() == ChangeRecord.Op.DELETE) {
				SCRIPT_VERSION_INDEX.remove(id);
				SCRIPT_CACHED_VERSION.remove(id);
				SCRIPT_CACHED_MD5.remove(id);
				removeOwnedScript(id);
			} else {
				Long cur = SCRIPT_VERSION_INDEX.get(id);
				// spec §8.5 幂等：缓存版本 ≥ 通知版本则忽略（DELETE 分支不受此限）
				if (cur != null && version <= cur) {
					return;
				}
				if (cur == null) {
					// 新增脚本：轮询/订阅的 ChangeRecord 不带 type/language 元数据，
					// 回源取一次注册影子 Node，否则引用它的 chain 在下次全量对账前都编译不过
					ScriptMeta meta = fetchScriptMetaWithRetry(id);
					if (meta == null) {
						return; // 已被删除/停用：等后续 DELETE 变更或对账处理
					}
					registerShadowScript(meta);
					if (!SHADOW_SCRIPTS.containsKey(id)) {
						return;
					}
				}
				SCRIPT_VERSION_INDEX.put(id, version);
				invalidateScriptCache(id);
			}
		}
	}

	/**
	 * 全量对账：以 manifest 为准修正索引与缓存。
	 * version 不一致触发 UPSERT（惰性失效）；version 相同再比 content_md5（双保险，spec §7），
	 * 发现"改了内容没动版本号"的脏写同样按 UPSERT 失效；清单中已消失的条目触发 DELETE；
	 * 新增 script（索引中无）登记影子。
	 * 由 {@link RuleDbSyncManager#reconcileOnce()} 回调。
	 */
	public static void reconcile(RuleManifest manifest) {
		validateManifest(manifest);
		validateManifestOwnership(manifest);
		Set<String> liveChains = new HashSet<>();
		if (manifest.getChains() != null) {
			for (ChainMeta cm : manifest.getChains()) {
				liveChains.add(cm.getChainId());
				Long cur = CHAIN_VERSION_INDEX.get(cm.getChainId());
				if (cur == null || cur != cm.getVersion()) {
					applyChange(new ChangeRecord(0, ChangeRecord.TargetType.CHAIN,
							cm.getChainId(), ChangeRecord.Op.UPSERT, cm.getVersion()));
				} else if (md5Mismatch(CHAIN_CACHED_MD5.get(cm.getChainId()), cm.getMd5())) {
					invalidateChainCache(cm.getChainId());
				}
			}
		}
		// 清单中已消失的 chain（且属 rule-db 管理）删除
		for (String chainId : new ArrayList<>(CHAIN_VERSION_INDEX.keySet())) {
			if (!liveChains.contains(chainId)) {
				applyChange(new ChangeRecord(0, ChangeRecord.TargetType.CHAIN,
						chainId, ChangeRecord.Op.DELETE, 0));
			}
		}
		Set<String> liveScripts = new HashSet<>();
		if (manifest.getScripts() != null) {
			for (ScriptMeta sm : manifest.getScripts()) {
				liveScripts.add(sm.getNodeId());
				Long cur = SCRIPT_VERSION_INDEX.get(sm.getNodeId());
				if (cur == null) {
					// 新增脚本：登记影子 Node + 版本戳
					registerShadowScript(sm);
					if (SHADOW_SCRIPTS.containsKey(sm.getNodeId())) {
						SCRIPT_VERSION_INDEX.put(sm.getNodeId(), sm.getVersion());
					}
				} else if (cur != sm.getVersion()) {
					applyChange(new ChangeRecord(0, ChangeRecord.TargetType.SCRIPT,
							sm.getNodeId(), ChangeRecord.Op.UPSERT, sm.getVersion()));
				} else if (md5Mismatch(SCRIPT_CACHED_MD5.get(sm.getNodeId()), sm.getMd5())) {
					invalidateScriptCache(sm.getNodeId());
				}
			}
		}
		for (String nodeId : new ArrayList<>(SCRIPT_VERSION_INDEX.keySet())) {
			if (!liveScripts.contains(nodeId)) {
				applyChange(new ChangeRecord(0, ChangeRecord.TargetType.SCRIPT,
						nodeId, ChangeRecord.Op.DELETE, 0));
			}
		}
	}

	/** 双方 md5 都在手才比较；缓存态没有 md5（影子/未回源）时不构成脏写信号 */
	private static boolean md5Mismatch(String cachedMd5, String manifestMd5) {
		return StrUtil.isNotBlank(cachedMd5) && StrUtil.isNotBlank(manifestMd5) && !cachedMd5.equals(manifestMd5);
	}

	static void validateManifest(RuleManifest manifest) {
		if (manifest == null) {
			throw new ConfigErrorException("rule-db manifest must not be null");
		}
		if (manifest.getLatestSeq() < 0) {
			throw new ConfigErrorException("rule-db manifest latestSeq must not be negative");
		}
		Set<String> chainIds = new HashSet<>();
		if (manifest.getChains() != null) {
			for (ChainMeta meta : manifest.getChains()) {
				if (meta == null || StrUtil.isBlank(meta.getChainId()) || meta.getVersion() <= 0) {
					throw new ConfigErrorException("rule-db manifest contains invalid chain metadata");
				}
				if (!chainIds.add(meta.getChainId())) {
					throw new ConfigErrorException("rule-db manifest contains duplicate chain[" + meta.getChainId() + "]");
				}
			}
		}
		Set<String> scriptIds = new HashSet<>();
		if (manifest.getScripts() != null) {
			for (ScriptMeta meta : manifest.getScripts()) {
				NodeTypeEnum type = meta == null ? null : NodeTypeEnum.getEnumByCode(meta.getType());
				if (meta == null || StrUtil.isBlank(meta.getNodeId()) || meta.getVersion() <= 0
						|| type == null || !type.isScript()) {
					throw new ConfigErrorException("rule-db manifest contains invalid script metadata");
				}
				if (!scriptIds.add(meta.getNodeId())) {
					throw new ConfigErrorException("rule-db manifest contains duplicate script[" + meta.getNodeId() + "]");
				}
			}
		}
	}

	private static void validateManifestOwnership(RuleManifest manifest) {
		if (manifest.getChains() != null) {
			for (ChainMeta meta : manifest.getChains()) {
				assertNoForeignChain(meta.getChainId());
			}
		}
		if (manifest.getScripts() != null) {
			for (ScriptMeta meta : manifest.getScripts()) {
				assertNoForeignScript(meta.getNodeId());
			}
		}
	}

	private static void assertNoForeignChain(String chainId) {
		Chain current = FlowBus.getChain(chainId);
		if (current != null && current != SHADOW_CHAINS.get(chainId)) {
			throw collision("chain", chainId);
		}
	}

	private static void assertNoForeignScript(String nodeId) {
		Node current = FlowBus.getNode(nodeId);
		if (current != null && current != SHADOW_SCRIPTS.get(nodeId)) {
			throw collision("script", nodeId);
		}
	}

	private static ConfigErrorException collision(String type, String id) {
		return new ConfigErrorException("rule-db " + type + "[" + id
				+ "] collides with application-owned FlowBus metadata");
	}

	private static void removeOwnedChain(String chainId) {
		Chain owned = SHADOW_CHAINS.remove(chainId);
		if (owned != null && FlowBus.getChain(chainId) == owned) {
			FlowBus.removeChain(chainId);
		}
	}

	private static void removeOwnedScript(String nodeId) {
		Node owned = SHADOW_SCRIPTS.remove(nodeId);
		if (owned != null && FlowBus.getNode(nodeId) == owned) {
			FlowBus.unloadScriptNode(nodeId);
		}
	}

	public static synchronized void destroy() {
		RuleDbSyncManager.stop();
		clearRuntimeState();
		// 重置 isActive 缓存，使下次 init 重新计算（非 rule-db 应用 destroy 后也不残留过期 true）
		activeFlag = null;
	}

	/** Clears all runtime-owned state after a failed activation or explicit destroy. */
	private static void clearRuntimeState() {
		RuleDbCache.destroy();
		for (Map.Entry<String, Chain> entry : SHADOW_CHAINS.entrySet()) {
			if (FlowBus.getChain(entry.getKey()) == entry.getValue()) {
				FlowBus.removeChain(entry.getKey());
			}
		}
		for (Map.Entry<String, Node> entry : SHADOW_SCRIPTS.entrySet()) {
			if (FlowBus.getNode(entry.getKey()) == entry.getValue()) {
				FlowBus.removeNode(entry.getKey());
			}
		}
		SHADOW_CHAINS.clear();
		SHADOW_SCRIPTS.clear();
		CHAIN_VERSION_INDEX.clear();
		SCRIPT_VERSION_INDEX.clear();
		CHAIN_CACHED_VERSION.clear();
		SCRIPT_CACHED_VERSION.clear();
		CHAIN_CACHED_MD5.clear();
		SCRIPT_CACHED_MD5.clear();
		LAST_APPLIED_SEQ.set(0);
		activeRepository = null;
		RuleRepositoryHolder.clearCached();
		initialized = false;
	}
}
