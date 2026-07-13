package com.yomahub.liteflow.repository.etcd;

import com.yomahub.liteflow.publisher.exception.RuleStorageException;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.TxnResponse;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

interface EtcdKvFacade {

	Value get(String key);

	Range range(String prefix);

	TxnResult putPair(String metadataKey, long expectedModRevision,
			String contentKey, String content, String metadata);

	TxnResult deletePair(String metadataKey, long expectedModRevision, String contentKey);

	final class Value {
		private final String value;
		private final long modRevision;
		private final long headerRevision;

		Value(String value, long modRevision, long headerRevision) {
			this.value = value;
			this.modRevision = modRevision;
			this.headerRevision = headerRevision;
		}

		String value() { return value; }
		long modRevision() { return modRevision; }
		long headerRevision() { return headerRevision; }
		boolean exists() { return value != null; }
	}

	final class Entry {
		private final String key;
		private final String value;
		private final long modRevision;

		Entry(String key, String value, long modRevision) {
			this.key = key;
			this.value = value;
			this.modRevision = modRevision;
		}

		String key() { return key; }
		String value() { return value; }
		long modRevision() { return modRevision; }
	}

	final class Range {
		private final List<Entry> entries;
		private final long revision;

		Range(List<Entry> entries, long revision) {
			this.entries = entries == null ? Collections.emptyList() : entries;
			this.revision = revision;
		}

		List<Entry> entries() { return entries; }
		long revision() { return revision; }
	}

	final class TxnResult {
		private final boolean succeeded;
		private final long revision;

		TxnResult(boolean succeeded, long revision) {
			this.succeeded = succeeded;
			this.revision = revision;
		}

		boolean succeeded() { return succeeded; }
		long revision() { return revision; }
	}
}

final class JetcdKvFacade implements EtcdKvFacade {

	private final KV kv;

	JetcdKvFacade(KV kv) {
		this.kv = kv;
	}

	@Override
	public Value get(String key) {
		GetResponse response = await(kv.get(bytes(key)), "get " + key);
		if (response.getKvs().isEmpty()) {
			return new Value(null, 0, response.getHeader().getRevision());
		}
		KeyValue value = response.getKvs().get(0);
		return new Value(text(value.getValue()), value.getModRevision(), response.getHeader().getRevision());
	}

	@Override
	public Range range(String prefix) {
		ByteSequence key = bytes(prefix);
		GetOption option = GetOption.newBuilder()
				.withPrefix(key)
				.withSortField(GetOption.SortTarget.KEY)
				.withSortOrder(GetOption.SortOrder.ASCEND)
				.build();
		GetResponse response = await(kv.get(key, option), "range " + prefix);
		List<Entry> entries = new ArrayList<>();
		for (KeyValue value : response.getKvs()) {
			entries.add(new Entry(text(value.getKey()), text(value.getValue()), value.getModRevision()));
		}
		return new Range(entries, response.getHeader().getRevision());
	}

	@Override
	public TxnResult putPair(String metadataKey, long expectedModRevision,
			String contentKey, String content, String metadata) {
		TxnResponse response = await(kv.txn()
				.If(new Cmp(bytes(metadataKey), Cmp.Op.EQUAL, CmpTarget.modRevision(expectedModRevision)))
				.Then(Op.put(bytes(contentKey), bytes(content), PutOption.DEFAULT),
						Op.put(bytes(metadataKey), bytes(metadata), PutOption.DEFAULT))
				.commit(), "put rule pair " + metadataKey);
		return new TxnResult(response.isSucceeded(), response.getHeader().getRevision());
	}

	@Override
	public TxnResult deletePair(String metadataKey, long expectedModRevision, String contentKey) {
		TxnResponse response = await(kv.txn()
				.If(new Cmp(bytes(metadataKey), Cmp.Op.EQUAL, CmpTarget.modRevision(expectedModRevision)))
				.Then(Op.delete(bytes(contentKey), DeleteOption.DEFAULT),
						Op.delete(bytes(metadataKey), DeleteOption.DEFAULT))
				.commit(), "delete rule pair " + metadataKey);
		return new TxnResult(response.isSucceeded(), response.getHeader().getRevision());
	}

	private <T> T await(java.util.concurrent.CompletableFuture<T> future, String operation) {
		try {
			return future.get();
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RuleStorageException("etcd " + operation + " interrupted", e);
		}
		catch (ExecutionException e) {
			Throwable cause = e.getCause() == null ? e : e.getCause();
			throw new RuleStorageException("etcd " + operation + " failed: " + cause.getMessage(), cause);
		}
	}

	private static ByteSequence bytes(String value) {
		return ByteSequence.from(value, StandardCharsets.UTF_8);
	}

	private static String text(ByteSequence value) {
		return value.toString(StandardCharsets.UTF_8);
	}
}
