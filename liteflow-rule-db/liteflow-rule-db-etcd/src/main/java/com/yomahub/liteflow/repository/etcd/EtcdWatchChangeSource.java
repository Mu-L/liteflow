package com.yomahub.liteflow.repository.etcd;

import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.log.LFLog;
import com.yomahub.liteflow.log.LFLoggerManager;
import com.yomahub.liteflow.repository.ChangeSourceHealth;
import com.yomahub.liteflow.repository.RuleChangeListener;
import com.yomahub.liteflow.repository.RuleChangeSource;
import com.yomahub.liteflow.repository.vo.ChangeRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

final class EtcdWatchChangeSource implements RuleChangeSource {

	private static final LFLog LOG = LFLoggerManager.getLogger(EtcdWatchChangeSource.class);

	private final Object monitor = new Object();
	private final EtcdWatchFacade watch;
	private final EtcdKeys keys;
	private final EtcdRecordCodec codec;
	private final TreeMap<Long, List<ChangeRecord>> buffered = new TreeMap<>();
	private final Map<String, Long> targetRevisions = new HashMap<>();
	private final List<EtcdWatchFacade.Handle> handles = new ArrayList<>();
	private final ExecutorService delivery = Executors.newSingleThreadExecutor(daemon("liteflow-rule-db-etcd-delivery"));
	private final ScheduledExecutorService retry = Executors.newSingleThreadScheduledExecutor(
			daemon("liteflow-rule-db-etcd-watch-retry"));

	private RuleChangeListener listener;
	private ChangeSourceHealth health = ChangeSourceHealth.starting();
	private long baseline;
	private long cursor;
	private int generation;
	private int retryAttempt;
	private boolean activated;
	private boolean recovering;
	private boolean closed;

	EtcdWatchChangeSource(EtcdWatchFacade watch, EtcdKeys keys, EtcdRecordCodec codec) {
		if (watch == null) {
			throw new ConfigErrorException("rule-db etcd watch client must not be null");
		}
		this.watch = watch;
		this.keys = keys;
		this.codec = codec;
	}

	@Override
	public void open(RuleChangeListener listener) {
		if (listener == null) {
			throw new IllegalArgumentException("rule change listener must not be null");
		}
		synchronized (monitor) {
			if (closed) {
				return;
			}
			this.listener = listener;
			openWatchesLocked(0);
		}
	}

	@Override
	public void activate(long baselineSeq) {
		List<ChangeRecord> replay = new ArrayList<>();
		synchronized (monitor) {
			if (closed || activated) {
				return;
			}
			baseline = baselineSeq;
			cursor = baselineSeq;
			activated = true;
			health = health.successful(cursor);
			for (Map.Entry<Long, List<ChangeRecord>> entry : buffered.entrySet()) {
				if (entry.getKey() > baselineSeq) {
					replay.addAll(entry.getValue());
				}
			}
			buffered.clear();
		}
		submit(replay);
	}

	@Override
	public boolean requiresContinuousSequence() {
		return false;
	}

	@Override
	public void onReconciled(long baselineSeq) {
		synchronized (monitor) {
			if (closed) {
				return;
			}
			baseline = baselineSeq;
			cursor = Math.max(cursor, baselineSeq);
			targetRevisions.clear();
			recovering = false;
			retryAttempt = 0;
			health = health.successful(cursor);
			generation++;
			closeHandlesLocked();
			openWatchesLocked(baselineSeq + 1);
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
		synchronized (monitor) {
			if (closed) {
				return;
			}
			closed = true;
			listener = null;
			generation++;
			closeHandlesLocked();
			buffered.clear();
			targetRevisions.clear();
			health = health.down(null);
		}
		retry.shutdownNow();
		delivery.shutdownNow();
	}

	private void openWatchesLocked(long startRevision) {
		if (closed || listener == null) {
			return;
		}
		int currentGeneration = ++generation;
		handles.add(watch.watch(keys.chainMetaPrefix(), startRevision,
				watchListener(currentGeneration, ChangeRecord.TargetType.CHAIN, keys.chainMetaPrefix())));
		handles.add(watch.watch(keys.scriptMetaPrefix(), startRevision,
				watchListener(currentGeneration, ChangeRecord.TargetType.SCRIPT, keys.scriptMetaPrefix())));
	}

	private EtcdWatchFacade.Listener watchListener(int expectedGeneration,
			ChangeRecord.TargetType targetType, String prefix) {
		return new EtcdWatchFacade.Listener() {
			@Override
			public void onEvents(List<EtcdWatchFacade.Event> events) {
				handleEvents(expectedGeneration, targetType, prefix, events);
			}

			@Override
			public void onError(Throwable error) {
				handleError(expectedGeneration, error);
			}
		};
	}

	private void handleEvents(int expectedGeneration, ChangeRecord.TargetType targetType,
			String prefix, List<EtcdWatchFacade.Event> events) {
		List<ChangeRecord> changes = new ArrayList<>();
		if (events != null) {
			for (EtcdWatchFacade.Event event : events) {
				String id = keys.idFrom(prefix, event.key());
				ChangeRecord.Op operation = event.delete() || !codec.enabled(event.value())
						? ChangeRecord.Op.DELETE : ChangeRecord.Op.UPSERT;
				long version = event.delete() ? 0 : codec.version(event.value());
				changes.add(new ChangeRecord(event.revision(), targetType, id, operation, version));
			}
		}
		synchronized (monitor) {
			if (closed || expectedGeneration != generation || changes.isEmpty()) {
				return;
			}
			retryAttempt = 0;
			if (!activated) {
				for (ChangeRecord change : changes) {
					buffered.computeIfAbsent(change.getSeq(), ignored -> new ArrayList<>()).add(change);
				}
				return;
			}
		}
		submit(changes);
	}

	private void submit(List<ChangeRecord> changes) {
		if (changes == null || changes.isEmpty()) {
			return;
		}
		delivery.execute(() -> deliver(changes));
	}

	private void deliver(List<ChangeRecord> changes) {
		RuleChangeListener activeListener;
		List<ChangeRecord> deliverable = new ArrayList<>();
		synchronized (monitor) {
			if (closed || !activated || listener == null) {
				return;
			}
			activeListener = listener;
			for (ChangeRecord change : changes) {
				String target = change.getTargetType() + ":" + change.getTargetId();
				long previous = targetRevisions.getOrDefault(target, baseline);
				if (change.getSeq() > previous) {
					deliverable.add(change);
				}
			}
		}
		if (deliverable.isEmpty()) {
			return;
		}
		try {
			activeListener.onChanges(deliverable);
			synchronized (monitor) {
				if (!closed) {
					for (ChangeRecord change : deliverable) {
						targetRevisions.put(change.getTargetType() + ":" + change.getTargetId(), change.getSeq());
						cursor = Math.max(cursor, change.getSeq());
					}
					health = health.successful(cursor);
				}
			}
		}
		catch (RuntimeException e) {
			requestReconcile(e);
		}
	}

	private void handleError(int expectedGeneration, Throwable error) {
		RuleChangeListener activeListener = null;
		boolean compacted;
		synchronized (monitor) {
			if (closed || expectedGeneration != generation || recovering) {
				return;
			}
			recovering = true;
			generation++;
			closeHandlesLocked();
			String message = error == null ? "etcd watch closed" : String.valueOf(error.getMessage());
			health = health.degraded(message);
			compacted = message.toLowerCase().contains("compact");
			if (compacted) {
				activeListener = listener;
			}
			else {
				scheduleRetryLocked();
			}
		}
		if (activeListener != null) {
			RuleChangeListener callback = activeListener;
			delivery.execute(callback::onReconcileRequired);
		}
	}

	private void scheduleRetryLocked() {
		long delayMillis = Math.min(5000L, 100L << Math.min(retryAttempt++, 5));
		retry.schedule(() -> {
			synchronized (monitor) {
				if (closed) {
					return;
				}
				recovering = false;
				openWatchesLocked(Math.max(1, cursor + 1));
			}
		}, delayMillis, TimeUnit.MILLISECONDS);
	}

	private void requestReconcile(RuntimeException failure) {
		RuleChangeListener activeListener;
		synchronized (monitor) {
			if (closed || listener == null) {
				return;
			}
			health = health.degraded(failure.getMessage());
			activeListener = listener;
		}
		LOG.warn("rule-db etcd change delivery failed: {}", failure.getMessage());
		activeListener.onReconcileRequired();
	}

	private void closeHandlesLocked() {
		List<EtcdWatchFacade.Handle> closing = new ArrayList<>(handles);
		handles.clear();
		for (EtcdWatchFacade.Handle handle : closing) {
			try {
				handle.close();
			}
			catch (RuntimeException ignored) {
				// Best effort; generation checks make late callbacks harmless.
			}
		}
	}

	private static ThreadFactory daemon(String name) {
		return runnable -> {
			Thread thread = new Thread(runnable, name);
			thread.setDaemon(true);
			return thread;
		};
	}
}
