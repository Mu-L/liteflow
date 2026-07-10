package com.yomahub.liteflow.repository;

import com.yomahub.liteflow.exception.SeqGapException;
import com.yomahub.liteflow.log.LFLog;
import com.yomahub.liteflow.log.LFLoggerManager;
import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import com.yomahub.liteflow.repository.vo.RuleManifest;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Rule-DB 变更同步：seq 轮询（主感知/兜底）+ 可选订阅推送 + 周期全量对账。
 *
 * <p>三路收敛：
 * <ul>
 *     <li>seq 轮询：按 {@code seqPollSeconds} 周期调 {@link RuleRepository#fetchChangesSince(long)}
 *     拉增量，逐条 {@link RuleDbRuntime#applyChange} 后推进 {@code LAST_APPLIED_SEQ}。</li>
 *     <li>订阅推送（可选，Redis 实现）：变更发生时立即回调，轮询之外的快速通道，同样推进 seq。</li>
 *     <li>全量对账：按 {@code reconcileSeconds} 周期拉 {@link RuleRepository#fetchManifest()}
 *     做 diff，覆盖丢消息/订阅断线/change_log 清理（seq 断档时立即触发一次）。</li>
 * </ul>
 *
 * <p>测试通过直接调用 {@link #pollOnce()} / {@link #reconcileOnce()} 绕过定时，保证确定性。
 *
 * @author Bryan.Zhang
 * @since 2.16.2
 */
public class RuleDbSyncManager {

	private static final LFLog LOG = LFLoggerManager.getLogger(RuleDbSyncManager.class);

	private static volatile ScheduledExecutorService pollScheduler;

	private static volatile ScheduledExecutorService reconcileScheduler;

	/** stop() 后置 false，使订阅回调在 destroy 之后不再重新填充缓存（spec §8.5 幂等） */
	private static volatile boolean running = false;

	public static synchronized void start() {
		running = true;
		RuleDbConfig cfg = LiteflowConfigGetter.get().getRuleDb();
		int seqPoll = seqPollSeconds(cfg);
		int reconcile = cfg == null || cfg.getReconcileSeconds() == null ? 60 : cfg.getReconcileSeconds();

		// 两个单线程调度器各自命名，便于线程转储区分轮询与对账任务
		pollScheduler = Executors.newSingleThreadScheduledExecutor(daemonFactory("liteflow-rule-db-sync-poll"));
		reconcileScheduler = Executors.newSingleThreadScheduledExecutor(daemonFactory("liteflow-rule-db-sync-reconcile"));
		pollScheduler.scheduleWithFixedDelay(RuleDbSyncManager::pollOnceSafe, seqPoll, seqPoll, TimeUnit.SECONDS);
		reconcileScheduler.scheduleWithFixedDelay(RuleDbSyncManager::reconcileOnceSafe, reconcile, reconcile, TimeUnit.SECONDS);

		// 可选订阅推送（Redis 实现；SQL/InMemory 空实现）；running 标志守卫，stop() 后回调 no-op
		try {
			RuleRepositoryHolder.get().subscribe(changes -> {
				if (!running) {
					return;
				}
				for (ChangeRecord c : changes) {
					RuleDbRuntime.applyChange(c);
					advanceSeq(c.getSeq());
				}
			});
		} catch (Exception e) {
			LOG.warn("rule-db subscribe failed, fallback to polling: {}", e.getMessage());
		}
	}

	private static int seqPollSeconds(RuleDbConfig cfg) {
		if (cfg != null && cfg.getSeqPollSeconds() != null) {
			return cfg.getSeqPollSeconds();
		}
		return 3; // 未配置时的通用默认；插件可通过配置覆盖（Redis 建议 30）
	}

	/**
	 * 单轮 seq 轮询：fetchLatestSeq → fetchChangesSince → 逐条 applyChange + 推进 seq。
	 * 检测到 seq 断档（SeqGapException）立即转全量对账。
	 * 公开以便测试直接驱动，绕过定时。
	 */
	public static void pollOnce() {
		RuleRepository repo = RuleRepositoryHolder.get();
		long last = RuleDbRuntime.LAST_APPLIED_SEQ.get();
		long latest = repo.fetchLatestSeq();
		if (latest <= last) {
			return;
		}
		try {
			List<ChangeRecord> changes = repo.fetchChangesSince(last);
			for (ChangeRecord c : changes) {
				RuleDbRuntime.applyChange(c);
				advanceSeq(c.getSeq());
			}
		} catch (SeqGapException gap) {
			LOG.warn("seq gap detected, trigger full reconcile: {}", gap.getMessage());
			reconcileOnce();
		}
	}

	/**
	 * 单轮全量对账：fetchManifest → reconcile + 推进 seq。
	 * 公开以便测试直接驱动，绕过定时。
	 */
	public static void reconcileOnce() {
		RuleManifest manifest = RuleRepositoryHolder.get().fetchManifest();
		RuleDbRuntime.reconcile(manifest);
		advanceSeq(manifest.getLatestSeq());
	}

	/**
	 * 单调推进 LAST_APPLIED_SEQ（CAS 自旋）：仅当新 seq 大于当前值才更新，
	 * 保证 seq 不回跳（订阅与轮询并发推进也安全）。
	 */
	private static void advanceSeq(long seq) {
		long cur;
		do {
			cur = RuleDbRuntime.LAST_APPLIED_SEQ.get();
			if (seq <= cur) {
				return;
			}
		} while (!RuleDbRuntime.LAST_APPLIED_SEQ.compareAndSet(cur, seq));
	}

	private static void pollOnceSafe() {
		try {
			pollOnce();
		} catch (Exception e) {
			LOG.warn("rule-db poll failed: {}", e.getMessage());
		}
	}

	private static void reconcileOnceSafe() {
		try {
			reconcileOnce();
		} catch (Exception e) {
			LOG.warn("rule-db reconcile failed: {}", e.getMessage());
		}
	}

	private static ThreadFactory daemonFactory(String name) {
		return r -> {
			Thread t = new Thread(r, name);
			t.setDaemon(true);
			return t;
		};
	}

	public static synchronized void stop() {
		// 先置 false：订阅回调若在 shutdown 期间触发则 no-op，避免 destroy 后重新填充缓存
		running = false;
		if (pollScheduler != null) {
			pollScheduler.shutdownNow();
			pollScheduler = null;
		}
		if (reconcileScheduler != null) {
			reconcileScheduler.shutdownNow();
			reconcileScheduler = null;
		}
	}
}
