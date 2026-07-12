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
import com.yomahub.liteflow.repository.runtime.RuleTargetState;
import com.yomahub.liteflow.repository.runtime.RuleTargetStatus;
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

	/** Per-target desired and active metadata retained independently of the execution cache. */
	private static final ConcurrentHashMap<String, RuleTargetState> CHAIN_STATES = new ConcurrentHashMap<>();
	private static final ConcurrentHashMap<String, RuleTargetState> SCRIPT_STATES = new ConcurrentHashMap<>();

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
			if (StrUtil.isNotBlank(trimmed) && isLive(CHAIN_STATES.get(trimmed))) {
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
		SCRIPT_STATES.compute(sm.getNodeId(), (id, state) -> desiredState(state, sm.getVersion(), sm.getMd5()));
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
			CHAIN_STATES.compute(chainId, (id, state) -> desiredState(state, cm.getVersion(), cm.getMd5()));
		}
	}

	private static RuleTargetState desiredState(RuleTargetState state, long version, String md5) {
		if (state == null || state.getStatus() == RuleTargetStatus.DELETED) {
			return new RuleTargetState(version, md5);
		}
		state.updateDesired(version, md5);
		return state;
	}

	private static boolean isLive(RuleTargetState state) {
		return state != null && state.getStatus() != RuleTargetStatus.DELETED;
	}

	/** buildUnCompileChain 回源钩子：影子或版本失效时，拉内容填 EL，交由后续既有编译逻辑 */
	public static void ensureChainLoaded(String chainId) {
		RuleTargetState state = CHAIN_STATES.get(chainId);
		if (!isLive(state)) {
			return; // 非 rule-db 管理的 chain（如手动 build），不干预
		}
		Chain chain = FlowBus.getChain(chainId);
		if (chain != null && StrUtil.isNotBlank(chain.getEl())
				&& (state.getStatus() == RuleTargetStatus.READY || state.isDesiredLoaded())) {
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
		state.markLoaded(record.getVersion(), record.getMd5());
	}

	/**
	 * compileScriptNode 回源钩子：脚本影子/失效时拉源码填入 node。
	 * 注意：EL 编译期 {@code OperatorHelper.convert} 会对 Node 做 clone，
	 * 故 compileScriptNode 收到的 node 往往是 nodeMap 中影子的副本——
	 * 必须把源码写到传入的 node 上，而非重新 FlowBus.getNode(nodeId)。
	 */
	public static void ensureScriptLoaded(Node node) {
		String nodeId = node.getId();
		RuleTargetState state = SCRIPT_STATES.get(nodeId);
		if (!isLive(state)) {
			return;
		}
		SHADOW_SCRIPTS.put(nodeId, node);
		if (StrUtil.isNotBlank(node.getScript())
				&& (state.getStatus() == RuleTargetStatus.READY || state.isDesiredLoaded())) {
			return;
		}
		ScriptRecord record = fetchScriptWithRetry(nodeId);
		if (record == null || !record.isEnable()) {
			throw new ChainLoadException(StrUtil.format("script node[{}] not found or disabled in rule repository", nodeId));
		}
		node.setScript(record.getScript());
		node.setLanguage(record.getLanguage());
		state.markLoaded(record.getVersion(), record.getMd5());
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

	private static ChainMeta fetchChainMetaWithRetry(String chainId) {
		int retry = retryTimes();
		RuntimeException last = null;
		for (int i = 0; i <= retry; i++) {
			try {
				return repositoryForRead().fetchChainMeta(chainId);
			} catch (RuntimeException e) {
				last = e;
			}
		}
		throw new ChainLoadException(StrUtil.format("fetch chain metadata[{}] failed after {} retries: {}",
				chainId, retry, last == null ? StrUtil.EMPTY : last.getMessage()));
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
		RuleTargetState state = CHAIN_STATES.get(chainId);
		if (state != null) {
			state.activateLoaded();
		}
		List<String> scriptRefs = new ArrayList<>();
		try {
			for (Node n : LiteflowMetaOperator.getNodes(chainId)) {
				if (n.getType() != null && n.getType().isScript() && isLive(SCRIPT_STATES.get(n.getId()))) {
					scriptRefs.add(n.getId());
				}
			}
		} catch (Exception ignored) {
		}
		RuleDbCache.recordChainAccess(chainId, scriptRefs);
	}

	public static void recordCompiledScript(String nodeId) {
		RuleTargetState state = SCRIPT_STATES.get(nodeId);
		if (state != null) {
			state.activateLoaded();
		}
	}

	public static void markChainLoadFailed(String chainId, Throwable error) {
		RuleTargetState state = CHAIN_STATES.get(chainId);
		if (state != null) {
			state.markFailed(error);
		}
	}

	public static void markScriptLoadFailed(String nodeId, Throwable error) {
		RuleTargetState state = SCRIPT_STATES.get(nodeId);
		if (state != null) {
			state.markFailed(error);
		}
	}

	public static boolean isChainStale(String chainId) {
		RuleTargetState state = CHAIN_STATES.get(chainId);
		return isLive(state) && state.getStatus() == RuleTargetStatus.STALE;
	}

	public static boolean isScriptStale(String nodeId) {
		RuleTargetState state = SCRIPT_STATES.get(nodeId);
		return isLive(state) && state.getStatus() == RuleTargetStatus.STALE;
	}

	// ---- 索引/缓存态操作，供 Task 4/5 的 Cache/SyncManager 使用 ----

	public static Long getChainVersion(String chainId) {
		RuleTargetState state = CHAIN_STATES.get(chainId);
		return isLive(state) ? state.getDesiredVersion() : null;
	}

	public static Map<String, Long> scriptVersionIndex() {
		Map<String, Long> versions = new ConcurrentHashMap<>();
		for (Map.Entry<String, RuleTargetState> entry : SCRIPT_STATES.entrySet()) {
			if (isLive(entry.getValue())) {
				versions.put(entry.getKey(), entry.getValue().getDesiredVersion());
			}
		}
		return versions;
	}

	public static RuleTargetState chainState(String chainId) {
		return CHAIN_STATES.get(chainId);
	}

	public static RuleTargetState scriptState(String nodeId) {
		return SCRIPT_STATES.get(nodeId);
	}

	static void onChainEvicted(String chainId) {
		RuleTargetState state = CHAIN_STATES.get(chainId);
		if (state != null) {
			state.clearActive();
		}
	}

	static void onScriptEvicted(String nodeId) {
		RuleTargetState state = SCRIPT_STATES.get(nodeId);
		if (state != null) {
			state.clearActive();
		}
	}

	/** Marks a loaded chain for refresh without discarding its active fields. */
	static void invalidateChainCache(String chainId) {
		Chain chain = FlowBus.getChain(chainId);
		if (chain != null) {
			chain.setCompiled(false);
		}
	}

	static void invalidateScriptCache(String nodeId) {
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
				CHAIN_STATES.computeIfAbsent(id, key -> new RuleTargetState()).markDeleted();
				removeOwnedChain(id);
			} else {
				RuleTargetState state = CHAIN_STATES.get(id);
				if (isLive(state) && version <= state.getDesiredVersion()) {
					return;
				}
				if (!isLive(state)) {
					ChainMeta meta = fetchChainMetaWithRetry(id);
					registerShadowChain(meta == null ? new ChainMeta(id, version, null) : meta);
					state = CHAIN_STATES.get(id);
					if (state != null && version > state.getDesiredVersion()) {
						state.updateDesired(version, null);
					}
					return;
				}
				state.updateDesired(version, null);
			}
		} else {
			assertNoForeignScript(id);
			if (change.getOp() == ChangeRecord.Op.DELETE) {
				SCRIPT_STATES.computeIfAbsent(id, key -> new RuleTargetState()).markDeleted();
				removeOwnedScript(id);
			} else {
				RuleTargetState state = SCRIPT_STATES.get(id);
				if (isLive(state) && version <= state.getDesiredVersion()) {
					return;
				}
				if (!isLive(state)) {
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
					state = SCRIPT_STATES.get(id);
					if (state != null && version > state.getDesiredVersion()) {
						state.updateDesired(version, null);
					}
					return;
				}
				state.updateDesired(version, null);
			}
		}
	}

	/**
	 * 全量对账：以 manifest 为准修正索引与缓存。
	 * version 不一致更新 desired 状态；version 相同再比 content_md5（双保险，spec §7），
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
				RuleTargetState state = CHAIN_STATES.get(cm.getChainId());
				if (!isLive(state)) {
					registerShadowChain(cm);
				} else if (cm.getVersion() > state.getDesiredVersion()
						|| (cm.getVersion() == state.getDesiredVersion()
						&& md5Mismatch(state.getDesiredMd5(), cm.getMd5()))) {
					state.updateDesired(cm.getVersion(), cm.getMd5());
				}
			}
		}
		// 清单中已消失的 chain（且属 rule-db 管理）删除
		for (String chainId : new ArrayList<>(CHAIN_STATES.keySet())) {
			if (isLive(CHAIN_STATES.get(chainId)) && !liveChains.contains(chainId)) {
				applyChange(new ChangeRecord(0, ChangeRecord.TargetType.CHAIN,
						chainId, ChangeRecord.Op.DELETE, 0));
			}
		}
		Set<String> liveScripts = new HashSet<>();
		if (manifest.getScripts() != null) {
			for (ScriptMeta sm : manifest.getScripts()) {
				liveScripts.add(sm.getNodeId());
				RuleTargetState state = SCRIPT_STATES.get(sm.getNodeId());
				if (!isLive(state)) {
					// 新增脚本：登记影子 Node + 版本戳
					registerShadowScript(sm);
				} else if (sm.getVersion() > state.getDesiredVersion()
						|| (sm.getVersion() == state.getDesiredVersion()
						&& md5Mismatch(state.getDesiredMd5(), sm.getMd5()))) {
					state.updateDesired(sm.getVersion(), sm.getMd5());
				}
			}
		}
		for (String nodeId : new ArrayList<>(SCRIPT_STATES.keySet())) {
			if (isLive(SCRIPT_STATES.get(nodeId)) && !liveScripts.contains(nodeId)) {
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
		CHAIN_STATES.clear();
		SCRIPT_STATES.clear();
		LAST_APPLIED_SEQ.set(0);
		activeRepository = null;
		RuleRepositoryHolder.clearCached();
		initialized = false;
	}
}
