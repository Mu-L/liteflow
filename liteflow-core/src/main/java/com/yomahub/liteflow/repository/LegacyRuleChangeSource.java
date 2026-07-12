package com.yomahub.liteflow.repository;

import com.yomahub.liteflow.exception.SeqGapException;
import com.yomahub.liteflow.log.LFLog;
import com.yomahub.liteflow.log.LFLoggerManager;
import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.repository.vo.ChangeRecord;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/** Adapter that preserves the transitional RuleRepository subscribe/poll APIs. */
final class LegacyRuleChangeSource implements RuleChangeSource, ManualPollingChangeSource {

	private static final LFLog LOG = LFLoggerManager.getLogger(LegacyRuleChangeSource.class);

	private final RuleRepository repository;
	private final Object monitor = new Object();
	private final List<ChangeRecord> buffered = new ArrayList<>();
	private RuleChangeListener listener;
	private ScheduledExecutorService pollScheduler;
	private long cursor;
	private boolean activated;
	private boolean closed;
	private boolean pendingReconcile;
	private ChangeSourceHealth health = ChangeSourceHealth.starting();

	LegacyRuleChangeSource(RuleRepository repository) {
		this.repository = repository;
	}

	@Override
	public void open(RuleChangeListener nextListener) {
		synchronized (monitor) {
			if (closed) {
				return;
			}
			listener = nextListener;
		}
		try {
			repository.subscribe(this::onLegacyChanges);
		} catch (RuntimeException e) {
			synchronized (monitor) {
				pendingReconcile = true;
			}
			markDegraded(e);
			LOG.warn("legacy rule-db subscribe failed; continuing with polling: {}", e.getMessage());
		}
	}

	private void onLegacyChanges(List<ChangeRecord> changes) {
		if (changes == null || changes.isEmpty()) {
			synchronized (monitor) {
				if (!activated) {
					pendingReconcile = true;
					return;
				}
			}
			requestReconcile("empty legacy change callback");
			return;
		}
		synchronized (monitor) {
			if (closed) {
				return;
			}
			buffered.addAll(changes);
			if (!activated) {
				return;
			}
		}
		drain();
	}

	@Override
	public void activate(long baselineSeq) {
		boolean reconcile;
		synchronized (monitor) {
			if (closed) {
				return;
			}
			cursor = baselineSeq;
			activated = true;
			health = health.successful(cursor);
			buffered.removeIf(c -> c == null || c.getSeq() <= cursor);
			reconcile = pendingReconcile;
			pendingReconcile = false;
		}
		drain();
		if (reconcile) {
			requestReconcile("pending pre-activation legacy signal");
		}
		startPolling();
	}

	@Override
	public void onReconciled(long baselineSeq) {
		synchronized (monitor) {
			if (closed) {
				return;
			}
			cursor = Math.max(cursor, baselineSeq);
			buffered.removeIf(c -> c == null || c.getSeq() <= cursor);
			pendingReconcile = false;
			health = health.successful(cursor);
		}
	}

	private void drain() {
		while (true) {
			List<ChangeRecord> batch;
			RuleChangeListener callback;
			synchronized (monitor) {
			if (closed || !activated || buffered.isEmpty() || listener == null) {
				return;
			}
				buffered.removeIf(c -> c == null || c.getSeq() <= cursor);
				if (buffered.isEmpty()) {
					return;
				}
				buffered.sort(Comparator.comparingLong(ChangeRecord::getSeq));
				batch = new ArrayList<>(buffered);
				callback = listener;
			}
			try {
				callback.onChanges(batch);
			} catch (RuntimeException e) {
				markDegraded(e);
				synchronized (monitor) {
					if (!closed) {
						// Keep the queued batch and cursor unchanged for retry/reconcile.
						buffered.removeIf(c -> c == null || c.getSeq() <= cursor);
					}
				}
				requestReconcile(callback, "legacy change delivery failure");
				LOG.warn("legacy rule-db change delivery failed: {}", e.getMessage());
				return;
			}
			long batchMaxSeq = batchMaxSeq(batch);
			if (RuleDbRuntime.LAST_APPLIED_SEQ.get() < batchMaxSeq) {
				markDegraded(new RuntimeException("legacy change batch was not applied through seq " + batchMaxSeq));
				LOG.warn("legacy rule-db change batch remains queued because applied cursor is {} but batch ends at {}",
						RuleDbRuntime.LAST_APPLIED_SEQ.get(), batchMaxSeq);
				return;
			}
			synchronized (monitor) {
				for (ChangeRecord change : batch) {
					if (change != null) {
						cursor = Math.max(cursor, change.getSeq());
					}
				}
				buffered.removeAll(batch);
				health = health.successful(cursor);
			}
		}
	}

	@Override
	public void pollOnce() {
		long since;
		boolean retryBuffered;
		synchronized (monitor) {
			if (closed || !activated) {
				return;
			}
			since = cursor;
			retryBuffered = !buffered.isEmpty();
		}
		try {
			if (retryBuffered) {
				drain();
				return;
			}
			if (repository.fetchLatestSeq() <= since) {
				synchronized (monitor) {
					health = health.successful(cursor);
				}
				return;
			}
			List<ChangeRecord> changes = repository.fetchChangesSince(since);
			onLegacyChanges(changes);
		} catch (SeqGapException gap) {
			markDegraded(gap);
			RuleChangeListener callback;
			synchronized (monitor) {
				callback = listener;
			}
			requestReconcile(callback, "legacy sequence gap");
		} catch (RuntimeException e) {
			markDegraded(e);
			LOG.warn("legacy rule-db poll failed: {}", e.getMessage());
		}
	}

	private long batchMaxSeq(List<ChangeRecord> batch) {
		long max = 0;
		for (ChangeRecord change : batch) {
			if (change != null) {
				max = Math.max(max, change.getSeq());
			}
		}
		return max;
	}

	private void startPolling() {
		synchronized (monitor) {
			if (closed || pollScheduler != null) {
				return;
			}
			RuleDbConfig config = LiteflowConfigGetter.get().getRuleDb();
			int seconds = config != null && config.getSeqPollSeconds() != null
					? config.getSeqPollSeconds() : repository.defaultSeqPollSeconds();
			pollScheduler = Executors.newSingleThreadScheduledExecutor(daemonFactory());
			pollScheduler.scheduleWithFixedDelay(this::pollOnce, seconds, seconds, TimeUnit.SECONDS);
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
		ScheduledExecutorService scheduler;
		synchronized (monitor) {
			if (closed) {
				return;
			}
			closed = true;
			listener = null;
			buffered.clear();
			scheduler = pollScheduler;
			pollScheduler = null;
			health = health.down(null);
		}
		if (scheduler != null) {
			scheduler.shutdownNow();
		}
	}

	private void requestReconcile(String reason) {
		RuleChangeListener callback;
		synchronized (monitor) {
			callback = listener;
		}
		requestReconcile(callback, reason);
	}

	private void requestReconcile(RuleChangeListener callback, String reason) {
		if (callback == null) {
			return;
		}
		try {
			callback.onReconcileRequired();
		} catch (RuntimeException e) {
			markDegraded(e);
			LOG.warn("legacy rule-db reconcile request failed ({}): {}", reason, e.getMessage());
		}
	}

	private void markDegraded(RuntimeException error) {
		synchronized (monitor) {
			if (!closed) {
				health = health.degraded(error.getMessage());
			}
		}
	}

	private ThreadFactory daemonFactory() {
		return r -> {
			Thread thread = new Thread(r, "liteflow-rule-db-legacy-poll");
			thread.setDaemon(true);
			return thread;
		};
	}
}
