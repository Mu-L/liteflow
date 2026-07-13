package com.yomahub.liteflow.repository.etcd;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class FakeEtcdKvFacade implements EtcdKvFacade {

	private final Map<String, Stored> values = new LinkedHashMap<>();
	private final List<String> rangePrefixes = new ArrayList<>();
	private final List<String> exactReads = new ArrayList<>();
	private long revision;
	private boolean failNextTransaction;

	@Override
	public synchronized Value get(String key) {
		exactReads.add(key);
		Stored stored = values.get(key);
		return stored == null ? new Value(null, 0, revision)
				: new Value(stored.value, stored.modRevision, revision);
	}

	@Override
	public synchronized Range range(String prefix) {
		rangePrefixes.add(prefix);
		List<Entry> entries = new ArrayList<>();
		List<String> keys = new ArrayList<>(values.keySet());
		Collections.sort(keys);
		for (String key : keys) {
			if (key.startsWith(prefix)) {
				Stored stored = values.get(key);
				entries.add(new Entry(key, stored.value, stored.modRevision));
			}
		}
		return new Range(entries, revision);
	}

	@Override
	public synchronized TxnResult putPair(String metadataKey, long expectedModRevision,
			String contentKey, String content, String metadata) {
		if (failNextTransaction) {
			failNextTransaction = false;
			return new TxnResult(false, revision);
		}
		Stored current = values.get(metadataKey);
		long actual = current == null ? 0 : current.modRevision;
		if (actual != expectedModRevision) {
			return new TxnResult(false, revision);
		}
		long next = ++revision;
		values.put(contentKey, new Stored(content, next));
		values.put(metadataKey, new Stored(metadata, next));
		return new TxnResult(true, next);
	}

	@Override
	public synchronized TxnResult deletePair(String metadataKey, long expectedModRevision, String contentKey) {
		Stored current = values.get(metadataKey);
		long actual = current == null ? 0 : current.modRevision;
		if (actual != expectedModRevision) {
			return new TxnResult(false, revision);
		}
		long next = ++revision;
		values.remove(metadataKey);
		values.remove(contentKey);
		return new TxnResult(true, next);
	}

	synchronized long putDirect(String key, String value) {
		long next = ++revision;
		values.put(key, new Stored(value, next));
		return next;
	}

	synchronized String raw(String key) {
		Stored value = values.get(key);
		return value == null ? null : value.value;
	}

	List<String> rangePrefixes() { return rangePrefixes; }
	List<String> exactReads() { return exactReads; }
	void failNextTransaction() { failNextTransaction = true; }

	private static final class Stored {
		private final String value;
		private final long modRevision;
		private Stored(String value, long modRevision) {
			this.value = value;
			this.modRevision = modRevision;
		}
	}
}
