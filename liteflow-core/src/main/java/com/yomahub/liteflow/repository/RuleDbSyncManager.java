package com.yomahub.liteflow.repository;

import com.yomahub.liteflow.log.LFLog;
import com.yomahub.liteflow.log.LFLoggerManager;
import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import com.yomahub.liteflow.repository.vo.RuleManifest;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Coordinates the two-phase change source lifecycle and manifest reconciliation.
 * Change source implementations own their backend polling or watch scheduling;
 * this class only serializes delivery and provides deterministic test hooks.
 *
 * @author Bryan.Zhang
 * @since 2.16.1
 */
public class RuleDbSyncManager {

	private static final LFLog LOG = LFLoggerManager.getLogger(RuleDbSyncManager.class);

	private static final Object CALLBACK_MONITOR = new Object();

	private static volatile ScheduledExecutorService reconcileScheduler;
	private static volatile RuleDbProvider provider;
	private static volatile RuleRepository repository;
	private static volatile RuleChangeSource changeSource;
	private static volatile boolean running;

	private static final RuleChangeListener LISTENER = new RuleChangeListener() {
		@Override
		public void onChanges(List<ChangeRecord> changes) {
			applyChanges(changes, true);
		}

		@Override
		public void onReconcileRequired() {
			synchronized (CALLBACK_MONITOR) {
				if (!running) {
					return;
				}
				reconcileNow();
			}
		}
	};

	private RuleDbSyncManager() {
	}

	/**
	 * Opens the provider change source before the initial manifest is fetched.
	 * Implementations must buffer callbacks until {@link #activate(long)}.
	 */
	public static synchronized void open(RuleDbProvider nextProvider) {
		if (running) {
			if (provider != nextProvider) {
				throw new IllegalStateException("rule-db sync manager is already open");
			}
			return;
		}
		if (nextProvider == null) {
			throw new IllegalArgumentException("rule-db provider must not be null");
		}
		RuleRepository nextRepository;
		RuleChangeSource nextSource;
		try {
			nextRepository = nextProvider.repository();
			nextSource = nextProvider.changeSource();
			if (nextRepository == null) {
				throw new IllegalArgumentException("rule-db provider repository must not be null");
			}
			if (nextSource == null) {
				nextSource = new NoopChangeSource();
			}
		} catch (RuntimeException e) {
			closeQuietly(nextProvider);
			RuleDbProviderHolder.clearIf(nextProvider);
			throw e;
		}
		provider = nextProvider;
		repository = nextRepository;
		changeSource = nextSource;
		running = true;
		try {
			nextSource.open(LISTENER);
		} catch (RuntimeException e) {
			closeQuietly(nextProvider);
			RuleDbProviderHolder.clearIf(nextProvider);
			running = false;
			provider = null;
			repository = null;
			changeSource = null;
			throw e;
		}
	}

	/** Activates the source at the manifest cursor and replays buffered events. */
	public static void activate(long baselineSeq) {
		RuleChangeSource source;
		synchronized (CALLBACK_MONITOR) {
			if (!running) {
				return;
			}
			advanceSeq(baselineSeq);
			source = changeSource;
		}
		if (source != null) {
			source.activate(baselineSeq);
		}
	}

	/** Starts only the periodic manifest reconciliation task. */
	public static synchronized void startReconcileScheduler() {
		if (!running || reconcileScheduler != null) {
			return;
		}
		RuleDbConfig cfg = LiteflowConfigGetter.get().getRuleDb();
		int reconcile = cfg == null || cfg.getReconcileSeconds() == null
				? 60 : cfg.getReconcileSeconds();
		reconcileScheduler = Executors.newSingleThreadScheduledExecutor(
				daemonFactory("liteflow-rule-db-sync-reconcile"));
		reconcileScheduler.scheduleWithFixedDelay(RuleDbSyncManager::reconcileOnceSafe,
				reconcile, reconcile, TimeUnit.SECONDS);
	}

	/** Transitional alias retained for callers that used the old lifecycle API. */
	public static void start() {
		startReconcileScheduler();
	}

	/**
	 * Deterministic polling hook. Backend change sources should schedule polling
	 * themselves; this method retains the transitional repository API for tests.
	 */
	public static void pollOnce() {
		RuleChangeSource source = changeSource;
		if (!(source instanceof ManualPollingChangeSource)) {
			return;
		}
		synchronized (CALLBACK_MONITOR) {
			if (!running) {
				return;
			}
			((ManualPollingChangeSource) source).pollOnce();
		}
	}

	/** Deterministic full-manifest reconciliation hook. */
	public static void reconcileOnce() {
		synchronized (CALLBACK_MONITOR) {
			if (!running) {
				return;
			}
			reconcileNow();
		}
	}

	private static void reconcileNow() {
		RuleRepository repo = repository;
		if (repo == null) {
			return;
		}
		RuleManifest manifest = repo.fetchManifest();
		RuleDbRuntime.reconcile(manifest);
		advanceSeq(manifest.getLatestSeq());
	}

	private static void applyChanges(List<ChangeRecord> changes, boolean requireRunning) {
		if (changes == null) {
			requestReconcile(requireRunning);
			return;
		}
		if (changes.isEmpty()) {
			return;
		}
		synchronized (CALLBACK_MONITOR) {
			if (requireRunning && !running) {
				return;
			}
			List<ChangeRecord> ordered = new ArrayList<>();
			for (ChangeRecord change : changes) {
				if (change != null) {
					ordered.add(change);
				}
			}
			if (ordered.isEmpty()) {
				requestReconcile(requireRunning);
				return;
			}
			ordered.sort(Comparator.comparingLong(ChangeRecord::getSeq));
			long initialCursor = RuleDbRuntime.LAST_APPLIED_SEQ.get();
			long nextCursor = initialCursor;
			try {
				for (ChangeRecord change : ordered) {
					if (change.getSeq() > 0 && change.getSeq() <= initialCursor) {
						continue;
					}
					RuleDbRuntime.applyChange(change);
					if (change.getSeq() > 0) {
						nextCursor = Math.max(nextCursor, change.getSeq());
					}
				}
				advanceSeq(nextCursor);
			} catch (RuntimeException applyFailure) {
				// The batch cursor is intentionally left unchanged. Reconcile from the
				// authoritative manifest so providers that cannot replay callbacks do
				// not silently lose the queued event.
				if (requireRunning && running) {
					try {
						reconcileNow();
					} catch (RuntimeException reconcileFailure) {
						LOG.warn("rule-db reconcile after change failure failed: {}",
								reconcileFailure.getMessage());
					}
				}
				throw applyFailure;
			}
		}
	}

	private static void requestReconcile(boolean requireRunning) {
		if (!requireRunning || !running) {
			return;
		}
		try {
			reconcileNow();
		} catch (RuntimeException reconcileFailure) {
			LOG.warn("rule-db reconcile after invalid change batch failed: {}",
				reconcileFailure.getMessage());
		}
	}

	private static void advanceSeq(long seq) {
		long cur;
		do {
			cur = RuleDbRuntime.LAST_APPLIED_SEQ.get();
			if (seq <= cur) {
				return;
			}
		} while (!RuleDbRuntime.LAST_APPLIED_SEQ.compareAndSet(cur, seq));
	}

	private static void reconcileOnceSafe() {
		if (!running) {
			return;
		}
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

	/** Stops scheduling and closes the provider-owned resources; late callbacks become no-ops. */
	public static synchronized void stop() {
		RuleDbProvider currentProvider;
		synchronized (CALLBACK_MONITOR) {
			running = false;
			currentProvider = provider;
			changeSource = null;
			provider = null;
			repository = null;
		}
		if (reconcileScheduler != null) {
			reconcileScheduler.shutdownNow();
			reconcileScheduler = null;
		}
		if (currentProvider != null) {
			closeQuietly(currentProvider);
			RuleDbProviderHolder.clearIf(currentProvider);
		}
	}

	static boolean isOpen() {
		return running;
	}

	static RuleRepository activeRepository() {
		return repository;
	}

	private static void closeQuietly(AutoCloseable closeable) {
		if (closeable == null) {
			return;
		}
		try {
			closeable.close();
		} catch (Exception e) {
			LOG.warn("rule-db resource close failed: {}", e.getMessage());
		}
	}

	private static final class NoopChangeSource implements RuleChangeSource {
		@Override
		public void open(RuleChangeListener listener) {
		}

		@Override
		public void activate(long baselineSeq) {
		}

		@Override
		public ChangeSourceHealth health() {
			return ChangeSourceHealth.starting();
		}
	}
}
