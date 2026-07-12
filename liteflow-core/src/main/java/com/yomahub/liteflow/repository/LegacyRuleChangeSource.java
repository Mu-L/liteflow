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
		repository.subscribe(this::onLegacyChanges);
	}

	private void onLegacyChanges(List<ChangeRecord> changes) {
		if (changes == null || changes.isEmpty()) {
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
		synchronized (monitor) {
			if (closed) {
				return;
			}
			cursor = baselineSeq;
			activated = true;
			buffered.removeIf(c -> c == null || c.getSeq() <= cursor);
		}
		drain();
		startPolling();
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
				synchronized (monitor) {
					if (!closed) {
						// Keep the queued batch and cursor unchanged for retry/reconcile.
						buffered.removeIf(c -> c == null || c.getSeq() <= cursor);
					}
				}
				if (!closed) {
					try {
						callback.onReconcileRequired();
					} catch (RuntimeException reconcileFailure) {
						LOG.warn("legacy rule-db reconcile failed: {}", reconcileFailure.getMessage());
					}
				}
				LOG.warn("legacy rule-db change delivery failed: {}", e.getMessage());
				return;
			}
			synchronized (monitor) {
				for (ChangeRecord change : batch) {
					if (change != null) {
						cursor = Math.max(cursor, change.getSeq());
					}
				}
				buffered.removeAll(batch);
			}
		}
	}

	@Override
	public void pollOnce() {
		long since;
		synchronized (monitor) {
			if (closed || !activated) {
				return;
			}
			since = cursor;
		}
		try {
			if (repository.fetchLatestSeq() <= since) {
				return;
			}
			List<ChangeRecord> changes = repository.fetchChangesSince(since);
			onLegacyChanges(changes);
		} catch (SeqGapException gap) {
			RuleChangeListener callback;
			synchronized (monitor) {
				callback = listener;
			}
			if (callback != null) {
				callback.onReconcileRequired();
			}
		} catch (RuntimeException e) {
			LOG.warn("legacy rule-db poll failed: {}", e.getMessage());
		}
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
			return ChangeSourceHealth.up(cursor);
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
		}
		if (scheduler != null) {
			scheduler.shutdownNow();
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
