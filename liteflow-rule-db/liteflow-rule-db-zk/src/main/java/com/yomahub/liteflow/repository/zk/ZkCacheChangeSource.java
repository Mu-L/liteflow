package com.yomahub.liteflow.repository.zk;

import com.yomahub.liteflow.log.LFLog;
import com.yomahub.liteflow.log.LFLoggerManager;
import com.yomahub.liteflow.repository.ChangeSourceHealth;
import com.yomahub.liteflow.repository.RuleChangeListener;
import com.yomahub.liteflow.repository.RuleChangeSource;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

final class ZkCacheChangeSource implements RuleChangeSource {

	private static final LFLog LOG = LFLoggerManager.getLogger(ZkCacheChangeSource.class);

	private final Object monitor = new Object();
	private final CuratorFramework client;
	private final ZkPaths paths;
	private final ZkRecordCodec codec;
	private final TreeMap<Long, List<ChangeRecord>> buffered = new TreeMap<>();
	private final Map<String, Long> targetRevisions = new HashMap<>();
	private final ExecutorService delivery = Executors.newSingleThreadExecutor(daemon());
	private final ConnectionStateListener connectionListener = this::onConnectionState;

	private CuratorCache chainCache;
	private CuratorCache scriptCache;
	private RuleChangeListener listener;
	private ChangeSourceHealth health = ChangeSourceHealth.starting();
	private long baseline;
	private long cursor;
	private boolean activated;
	private boolean opened;
	private boolean closed;

	ZkCacheChangeSource(CuratorFramework client, ZkPaths paths, ZkRecordCodec codec) {
		this.client = client;
		this.paths = paths;
		this.codec = codec;
	}

	@Override
	public void open(RuleChangeListener listener) {
		if (listener == null) { throw new IllegalArgumentException("rule change listener must not be null"); }
		synchronized (monitor) {
			if (closed || opened) { return; }
			this.listener = listener;
			opened = true;
			try {
				chainCache = cache(paths.chainMetaRoot(), ChangeRecord.TargetType.CHAIN);
				scriptCache = cache(paths.scriptMetaRoot(), ChangeRecord.TargetType.SCRIPT);
				client.getConnectionStateListenable().addListener(connectionListener);
				chainCache.start();
				scriptCache.start();
			}
			catch (RuntimeException e) {
				// reset so a later open() can retry instead of being silently ignored
				client.getConnectionStateListenable().removeListener(connectionListener);
				closeQuietly(chainCache);
				closeQuietly(scriptCache);
				chainCache = null;
				scriptCache = null;
				this.listener = null;
				opened = false;
				throw e;
			}
		}
	}

	@Override
	public void activate(long baselineSeq) {
		List<ChangeRecord> replay = new ArrayList<>();
		synchronized (monitor) {
			if (closed || activated) { return; }
			baseline = baselineSeq;
			cursor = baselineSeq;
			activated = true;
			health = health.successful(cursor);
			for (Map.Entry<Long, List<ChangeRecord>> entry : buffered.entrySet()) {
				if (entry.getKey() > baselineSeq) { replay.addAll(entry.getValue()); }
			}
			buffered.clear();
		}
		submit(() -> {
			deliver(replay);
			requestReconcile();
		});
	}

	@Override
	public boolean requiresContinuousSequence() { return false; }

	@Override
	public void onReconciled(long baselineSeq) {
		synchronized (monitor) {
			if (closed) { return; }
			baseline = baselineSeq;
			cursor = Math.max(cursor, baselineSeq);
			targetRevisions.clear();
			health = health.successful(cursor);
		}
	}

	@Override
	public ChangeSourceHealth health() {
		synchronized (monitor) { return health; }
	}

	@Override
	public void close() {
		CuratorCache chains;
		CuratorCache scripts;
		synchronized (monitor) {
			if (closed) { return; }
			closed = true;
			opened = false;
			listener = null;
			buffered.clear();
			targetRevisions.clear();
			health = health.down(null);
			chains = chainCache;
			scripts = scriptCache;
			chainCache = null;
			scriptCache = null;
		}
		client.getConnectionStateListenable().removeListener(connectionListener);
		if (chains != null) { chains.close(); }
		if (scripts != null) { scripts.close(); }
		delivery.shutdownNow();
	}

	private CuratorCache cache(String root, ChangeRecord.TargetType targetType) {
		CuratorCache cache = CuratorCache.build(client, root);
		cache.listenable().addListener((type, oldData, data) -> onEvent(root, targetType, type, oldData, data));
		return cache;
	}

	void onEvent(String root, ChangeRecord.TargetType targetType,
			CuratorCacheListener.Type eventType, ChildData oldData, ChildData data) {
		ChildData current = eventType == CuratorCacheListener.Type.NODE_DELETED ? oldData : data;
		if (current == null || current.getPath().equals(root)) { return; }
		try {
			String id = paths.idFrom(root, current.getPath());
			boolean deleted = eventType == CuratorCacheListener.Type.NODE_DELETED;
			long revision;
			if (deleted) {
				revision = deletionRevision(root);
				if (revision < 0) {
					// never guess a sequence for a delete: a fabricated one can be silently
					// dropped by delivery de-duplication. reconcile instead.
					markDegraded("cannot resolve delete revision for " + current.getPath());
					requestReconcile();
					return;
				}
			}
			else {
				revision = current.getStat().getMzxid();
			}
			long version = current.getData() == null ? 0 : codec.version(current.getData());
			ChangeRecord.Op operation = deleted || !codec.enabled(current.getData())
					? ChangeRecord.Op.DELETE : ChangeRecord.Op.UPSERT;
			onChange(new ChangeRecord(revision, targetType, id, operation, version));
		}
		catch (RuntimeException e) {
			markDegraded(e.getMessage());
			requestReconcile();
		}
	}

	private long deletionRevision(String root) {
		try {
			Stat rootStat = client.checkExists().forPath(root);
			if (rootStat != null) { return Math.max(rootStat.getPzxid(), rootStat.getMzxid()); }
			LOG.warn("rule-db zk metadata root {} is missing while resolving delete revision", root);
		}
		catch (Exception e) {
			LOG.warn("rule-db zk cannot read delete revision for {}: {}", root, e.getMessage());
		}
		return -1;
	}

	private void onChange(ChangeRecord change) {
		synchronized (monitor) {
			if (closed) { return; }
			if (!activated) {
				buffered.computeIfAbsent(change.getSeq(), ignored -> new ArrayList<>()).add(change);
				return;
			}
		}
		List<ChangeRecord> changes = new ArrayList<>(1);
		changes.add(change);
		submit(() -> deliver(changes));
	}

	private void deliver(List<ChangeRecord> changes) {
		if (changes == null || changes.isEmpty()) { return; }
		RuleChangeListener activeListener;
		List<ChangeRecord> deliverable = new ArrayList<>();
		synchronized (monitor) {
			if (closed || !activated || listener == null) { return; }
			activeListener = listener;
			for (ChangeRecord change : changes) {
				String target = change.getTargetType() + ":" + change.getTargetId();
				long previous = targetRevisions.getOrDefault(target, baseline);
				if (change.getSeq() > previous) { deliverable.add(change); }
			}
		}
		if (deliverable.isEmpty()) { return; }
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
			markDegraded(e.getMessage());
			requestReconcile();
		}
	}

	private void onConnectionState(CuratorFramework ignored, ConnectionState state) {
		if (state == ConnectionState.SUSPENDED || state == ConnectionState.LOST) {
			markDegraded("ZooKeeper connection " + state.name().toLowerCase());
		}
		else if (state == ConnectionState.RECONNECTED) {
			submit(this::requestReconcile);
		}
	}

	private void submit(Runnable task) {
		synchronized (monitor) {
			if (closed) { return; }
			delivery.execute(task);
		}
	}

	private void requestReconcile() {
		RuleChangeListener activeListener;
		synchronized (monitor) {
			if (closed || !activated || listener == null) { return; }
			activeListener = listener;
		}
		activeListener.onReconcileRequired();
	}

	private void markDegraded(String message) {
		synchronized (monitor) {
			if (!closed) { health = health.degraded(message); }
		}
	}

	private static void closeQuietly(CuratorCache cache) {
		if (cache != null) {
			try { cache.close(); }
			catch (RuntimeException ignored) { }
		}
	}

	private static ThreadFactory daemon() {
		return runnable -> {
			Thread thread = new Thread(runnable, "liteflow-rule-db-zk-delivery");
			thread.setDaemon(true);
			return thread;
		};
	}
}
