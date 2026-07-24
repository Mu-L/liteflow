package com.yomahub.liteflow.repository.nacos;

import com.yomahub.liteflow.log.LFLog;
import com.yomahub.liteflow.log.LFLoggerManager;
import com.yomahub.liteflow.repository.ChangeSourceHealth;
import com.yomahub.liteflow.repository.RuleChangeListener;
import com.yomahub.liteflow.repository.RuleChangeSource;
import com.yomahub.liteflow.repository.vo.ChangeRecord;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;

/** Nacos listener with buffered activation and sequence-gap reconciliation. */
final class NacosListenerChangeSource implements RuleChangeSource {

	private static final LFLog LOG = LFLoggerManager.getLogger(NacosListenerChangeSource.class);

	private final Object monitor = new Object();
	private final NacosCatalogStore store;
	private final TreeMap<Long, ChangeRecord> buffered = new TreeMap<>();
	private final ExecutorService delivery = Executors.newSingleThreadExecutor(daemon());

	private NacosConfigFacade.Subscription subscription;
	private RuleChangeListener listener;
	private ChangeSourceHealth health = ChangeSourceHealth.starting();
	private long cursor;
	private boolean opened;
	private boolean activated;
	private boolean reconciling;
	private boolean drainScheduled;
	private boolean closed;

	NacosListenerChangeSource(NacosCatalogStore store) {
		this.store = store;
	}

	@Override
	public void open(RuleChangeListener listener) {
		if (listener == null) {
			throw new IllegalArgumentException("rule change listener must not be null");
		}
		synchronized (monitor) {
			if (closed || opened) { return; }
			this.listener = listener;
			opened = true;
		}
		NacosConfigFacade.Subscription next;
		try {
			next = store.subscribe(this::onContent);
		}
		catch (RuntimeException e) {
			synchronized (monitor) {
				this.listener = null;
				opened = false;
			}
			throw e;
		}
		synchronized (monitor) {
			if (closed) {
				next.close();
				return;
			}
			subscription = next;
		}
		onContent(next.initialContent());
	}

	@Override
	public void activate(long baselineSeq) {
		synchronized (monitor) {
			if (closed || activated) { return; }
			cursor = baselineSeq;
			activated = true;
			health = health.successful(cursor);
			buffered.headMap(cursor, true).clear();
			scheduleDrainLocked();
		}
	}

	@Override
	public void onReconciled(long baselineSeq) {
		synchronized (monitor) {
			if (closed) { return; }
			cursor = baselineSeq;
			buffered.headMap(cursor, true).clear();
			reconciling = false;
			health = health.successful(cursor);
			scheduleDrainLocked();
		}
	}

	@Override
	public ChangeSourceHealth health() {
		synchronized (monitor) {
			return health;
		}
	}

	@Override
	public void close() {
		NacosConfigFacade.Subscription closing;
		synchronized (monitor) {
			if (closed) { return; }
			closed = true;
			opened = false;
			listener = null;
			buffered.clear();
			health = health.down(null);
			closing = subscription;
			subscription = null;
		}
		if (closing != null) {
			closing.close();
		}
		delivery.shutdownNow();
	}

	private void onContent(String content) {
		try {
			NacosCatalog catalog = store.accept(content);
			ChangeRecord change = catalog.lastChange();
			if (change == null) { return; }
			synchronized (monitor) {
				if (closed || change.getSeq() <= cursor) { return; }
				buffered.put(change.getSeq(), change);
				if (activated) { scheduleDrainLocked(); }
			}
		}
		catch (RuntimeException e) {
			signalReconcile(e.getMessage());
		}
	}

	private void scheduleDrainLocked() {
		if (closed || !activated || reconciling || drainScheduled || buffered.isEmpty()) { return; }
		drainScheduled = true;
		try {
			delivery.execute(this::drain);
		}
		catch (RejectedExecutionException ignored) {
			drainScheduled = false;
		}
	}

	private void drain() {
		while (true) {
			ChangeRecord change;
			RuleChangeListener activeListener;
			synchronized (monitor) {
				buffered.headMap(cursor, true).clear();
				if (closed || !activated || reconciling || buffered.isEmpty() || listener == null) {
					drainScheduled = false;
					return;
				}
				Map.Entry<Long, ChangeRecord> first = buffered.firstEntry();
				if (first.getKey() != cursor + 1) {
					drainScheduled = false;
					startReconcileLocked("Nacos listener sequence gap: expected="
							+ (cursor + 1) + " actual=" + first.getKey());
					return;
				}
				change = first.getValue();
				activeListener = listener;
			}
			try {
				activeListener.onChanges(Collections.singletonList(change));
			}
			catch (RuntimeException e) {
				synchronized (monitor) {
					drainScheduled = false;
					startReconcileLocked(e.getMessage());
				}
				return;
			}
			synchronized (monitor) {
				if (closed) {
					drainScheduled = false;
					return;
				}
				buffered.remove(change.getSeq());
				cursor = change.getSeq();
				health = health.successful(cursor);
			}
		}
	}

	private void signalReconcile(String error) {
		synchronized (monitor) {
			if (closed || listener == null || reconciling) { return; }
			startReconcileLocked(error);
		}
	}

	private void startReconcileLocked(String error) {
		reconciling = true;
		health = health.degraded(error);
		RuleChangeListener activeListener = listener;
		if (activeListener == null) { return; }
		try {
			delivery.execute(() -> {
				try {
					activeListener.onReconcileRequired();
				}
				catch (RuntimeException e) {
					LOG.warn("rule-db nacos reconciliation callback failed: {}", e.getMessage());
				}
			});
		}
		catch (RejectedExecutionException ignored) {
			// close() raced with the callback.
		}
	}

	private static ThreadFactory daemon() {
		return runnable -> {
			Thread thread = new Thread(runnable, "liteflow-rule-db-nacos-delivery");
			thread.setDaemon(true);
			return thread;
		};
	}
}
