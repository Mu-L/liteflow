package com.yomahub.liteflow.repository.nacos;

import com.yomahub.liteflow.publisher.exception.RuleStorageException;

/** Shared authoritative snapshot used by the repository and listener. */
final class NacosCatalogStore {

	private final NacosConfigFacade facade;
	private final NacosConfigKey key;
	private final long timeoutMillis;
	private final NacosCatalogCodec codec;

	private Snapshot current;

	NacosCatalogStore(NacosConfigFacade facade, NacosConfigKey key,
			long timeoutMillis, NacosCatalogCodec codec) {
		this.facade = facade;
		this.key = key;
		this.timeoutMillis = timeoutMillis;
		this.codec = codec;
	}

	synchronized NacosCatalog refresh() {
		// Order the remote read with listener callbacks so an older in-flight read
		// cannot be mistaken for an authoritative catalog rollback.
		String content = facade.get(key.dataId(), key.group(), timeoutMillis);
		return accept(content);
	}

	synchronized NacosCatalog accept(String content) {
		NacosCatalog catalog = codec.decode(content);
		String md5 = codec.md5(content);
		if (current != null) {
			if (catalog.sequence() < current.sequence) {
				throw new RuleStorageException("Nacos rule catalog sequence moved backwards from "
						+ current.sequence + " to " + catalog.sequence());
			}
			if (catalog.sequence() == current.sequence && !md5.equals(current.md5)) {
				throw new RuleStorageException("Nacos rule catalog content changed without advancing sequence["
						+ catalog.sequence() + "]");
			}
		}
		current = new Snapshot(catalog.sequence(), md5);
		return catalog;
	}

	NacosConfigFacade.Subscription subscribe(java.util.function.Consumer<String> consumer) {
		return facade.subscribe(key.dataId(), key.group(), timeoutMillis, consumer);
	}

	private static final class Snapshot {
		private final long sequence;
		private final String md5;

		private Snapshot(long sequence, String md5) {
			this.sequence = sequence;
			this.md5 = md5;
		}
	}
}
