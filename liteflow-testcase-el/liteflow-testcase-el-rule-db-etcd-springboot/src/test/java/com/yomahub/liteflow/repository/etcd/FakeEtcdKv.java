package com.yomahub.liteflow.repository.etcd;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * MVCC-capable in-memory {@link EtcdKvFacade} for unit tests. Every mutation
 * bumps a store revision and keeps per-key history so pinned-revision range
 * reads behave like etcd snapshots.
 */
final class FakeEtcdKv implements EtcdKvFacade {

	private final Map<String, List<Versioned>> history = new LinkedHashMap<>();
	private final List<String> rangePrefixes = new ArrayList<>();
	// A newly initialized etcd store reports revision 1, including for missing-key reads.
	private long revision = 1;
	private int failTransactions;
	private String onceRangePrefix;
	private Runnable onceRangeAction;
	private String onceGetKey;
	private Runnable onceGetAction;

	@Override
	public synchronized Value get(String key) {
		if (key.equals(onceGetKey)) {
			Runnable action = onceGetAction;
			onceGetKey = null;
			onceGetAction = null;
			action.run();
		}
		Versioned latest = latestAt(key, revision);
		return latest == null || latest.value == null
				? new Value(null, 0, revision)
				: new Value(latest.value, latest.modRevision, revision);
	}

	@Override
	public synchronized Range range(String prefix) {
		return rangeAt(prefix, revision);
	}

	@Override
	public synchronized Range range(String prefix, long pinnedRevision) {
		return rangeAt(prefix, pinnedRevision);
	}

	private Range rangeAt(String prefix, long atRevision) {
		if (prefix.equals(onceRangePrefix)) {
			Runnable action = onceRangeAction;
			onceRangePrefix = null;
			onceRangeAction = null;
			action.run();
		}
		rangePrefixes.add(prefix);
		List<String> sortedKeys = new ArrayList<>(history.keySet());
		Collections.sort(sortedKeys);
		List<Entry> entries = new ArrayList<>();
		for (String key : sortedKeys) {
			if (key.startsWith(prefix)) {
				Versioned versioned = latestAt(key, atRevision);
				if (versioned != null && versioned.value != null) {
					entries.add(new Entry(key, versioned.value, versioned.modRevision));
				}
			}
		}
		return new Range(entries, atRevision);
	}

	@Override
	public synchronized TxnResult putPair(String metadataKey, long expectedModRevision,
			String contentKey, String content, String metadata) {
		if (failTransactions > 0) {
			failTransactions--;
			return new TxnResult(false, revision);
		}
		if (modRevisionOf(metadataKey) != expectedModRevision) {
			return new TxnResult(false, revision);
		}
		long next = ++revision;
		append(contentKey, content, next);
		append(metadataKey, metadata, next);
		return new TxnResult(true, next);
	}

	@Override
	public synchronized TxnResult deletePair(String metadataKey, long expectedModRevision, String contentKey) {
		if (failTransactions > 0) {
			failTransactions--;
			return new TxnResult(false, revision);
		}
		if (modRevisionOf(metadataKey) != expectedModRevision) {
			return new TxnResult(false, revision);
		}
		long next = ++revision;
		append(contentKey, null, next);
		append(metadataKey, null, next);
		return new TxnResult(true, next);
	}

	synchronized void putDirect(String key, String value) {
		append(key, value, ++revision);
	}

	synchronized void removeDirect(String key) {
		append(key, null, ++revision);
	}

	synchronized String raw(String key) {
		Versioned latest = latestAt(key, revision);
		return latest == null ? null : latest.value;
	}

	synchronized long currentRevision() {
		return revision;
	}

	synchronized List<String> rangePrefixes() {
		return new ArrayList<>(rangePrefixes);
	}

	synchronized void failNextTransactions(int count) {
		failTransactions = count;
	}

	/** Runs {@code action} once, just before the next range read on {@code prefix}. */
	synchronized void onceOnRange(String prefix, Runnable action) {
		onceRangePrefix = prefix;
		onceRangeAction = action;
	}

	/** Runs {@code action} once, just before the next get of {@code key}. */
	synchronized void onceOnGet(String key, Runnable action) {
		onceGetKey = key;
		onceGetAction = action;
	}

	private long modRevisionOf(String key) {
		Versioned latest = latestAt(key, revision);
		return latest == null || latest.value == null ? 0 : latest.modRevision;
	}

	private Versioned latestAt(String key, long atRevision) {
		List<Versioned> versions = history.get(key);
		if (versions == null) {
			return null;
		}
		for (int i = versions.size() - 1; i >= 0; i--) {
			if (versions.get(i).modRevision <= atRevision) {
				return versions.get(i);
			}
		}
		return null;
	}

	private void append(String key, String value, long modRevision) {
		history.computeIfAbsent(key, ignored -> new ArrayList<>()).add(new Versioned(modRevision, value));
	}

	private static final class Versioned {
		private final long modRevision;
		private final String value;
		private Versioned(long modRevision, String value) {
			this.modRevision = modRevision;
			this.value = value;
		}
	}
}
