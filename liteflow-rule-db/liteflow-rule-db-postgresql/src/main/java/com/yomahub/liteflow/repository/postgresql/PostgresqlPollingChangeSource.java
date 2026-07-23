package com.yomahub.liteflow.repository.postgresql;

import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.log.LFLog;
import com.yomahub.liteflow.log.LFLoggerManager;
import com.yomahub.liteflow.repository.ChangeSourceHealth;
import com.yomahub.liteflow.repository.ManualPollingChangeSource;
import com.yomahub.liteflow.repository.RuleChangeListener;
import com.yomahub.liteflow.repository.RuleChangeSource;
import com.yomahub.liteflow.repository.vo.ChangeRecord;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Incremental PostgreSQL change-log poller with manifest reconciliation fallback. */
public final class PostgresqlPollingChangeSource implements RuleChangeSource, ManualPollingChangeSource {

	private static final LFLog LOG = LFLoggerManager.getLogger(PostgresqlPollingChangeSource.class);
	private final Object monitor = new Object();
	private final Object pollMonitor = new Object();
	private final PostgresqlRuleRepository repository;
	private final int pollSeconds;
	private final int batchSize;
	private RuleChangeListener listener;
	private ScheduledExecutorService scheduler;
	private ChangeSourceHealth health = ChangeSourceHealth.starting();
	private long cursor;
	private boolean activated;
	private boolean closed;

	public PostgresqlPollingChangeSource(PostgresqlRuleRepository repository, int pollSeconds, int batchSize) {
		if (repository == null) { throw new ConfigErrorException("rule-db postgresql repository must not be null"); }
		if (pollSeconds <= 0) { throw new ConfigErrorException("liteflow.rule-db.sync.poll-seconds must be positive"); }
		if (batchSize <= 0) { throw new ConfigErrorException("liteflow.rule-db.postgresql.change-log-batch-size must be positive"); }
		this.repository = repository;
		this.pollSeconds = pollSeconds;
		this.batchSize = batchSize;
	}

	@Override public boolean requiresContinuousSequence() { return false; }

	@Override
	public void open(RuleChangeListener listener) {
		if (listener == null) { throw new IllegalArgumentException("rule change listener must not be null"); }
		synchronized (monitor) { if (!closed) { this.listener = listener; } }
	}

	@Override
	public void activate(long baselineSeq) {
		synchronized (monitor) {
			if (closed || activated) { return; }
			cursor = baselineSeq;
			activated = true;
			health = health.successful(cursor);
			scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
				Thread thread = new Thread(runnable, "liteflow-rule-db-postgresql-poll");
				thread.setDaemon(true);
				return thread;
			});
			scheduler.scheduleWithFixedDelay(this::pollSafely, pollSeconds, pollSeconds, TimeUnit.SECONDS);
		}
	}

	@Override public void pollOnce() { synchronized (pollMonitor) { doPoll(); } }

	private void doPoll() {
		RuleChangeListener activeListener;
		long baseline;
		synchronized (monitor) {
			if (closed || !activated || listener == null) { return; }
			activeListener = listener;
			baseline = cursor;
		}
		try {
			long highWatermark = repository.fetchLatestSeq();
			while (baseline < highWatermark) {
				List<ChangeRecord> changes = repository.fetchChangesSince(baseline, batchSize);
				if (changes.isEmpty()) {
					markDegraded("rule-db postgresql change-log advanced but no rows were readable");
					activeListener.onReconcileRequired();
					return;
				}
				if (isClosed()) { return; }
				activeListener.onChanges(changes);
				long delivered = baseline;
				for (ChangeRecord change : changes) { delivered = Math.max(delivered, change.getSeq()); }
				if (delivered <= baseline) { throw new IllegalStateException("rule-db postgresql cursor did not advance"); }
				baseline = delivered;
				synchronized (monitor) { if (!closed) { cursor = Math.max(cursor, delivered); health = health.successful(cursor); } }
			}
			markSuccessful();
		}
		catch (PostgresqlChangeLogCorruptionException e) {
			markDegraded(e.getMessage());
			if (!isClosed()) { activeListener.onReconcileRequired(); }
		}
		catch (RuntimeException e) { markDegraded(e.getMessage()); throw e; }
	}

	@Override
	public void onReconciled(long baselineSeq) {
		synchronized (monitor) { if (!closed) { cursor = baselineSeq; health = health.successful(cursor); } }
	}

	@Override public ChangeSourceHealth health() { synchronized (monitor) { return health; } }

	@Override
	public void close() {
		synchronized (monitor) {
			if (closed) { return; }
			closed = true;
			listener = null;
			if (scheduler != null) { scheduler.shutdownNow(); scheduler = null; }
			health = health.down(null);
		}
	}

	private void pollSafely() {
		try { pollOnce(); }
		catch (RuntimeException e) { LOG.warn("rule-db postgresql poll failed: {}", e.getMessage()); }
	}

	private void markSuccessful() { synchronized (monitor) { if (!closed) { health = health.successful(cursor); } } }
	private void markDegraded(String error) { synchronized (monitor) { if (!closed) { health = health.degraded(error); } } }
	private boolean isClosed() { synchronized (monitor) { return closed; } }
}
