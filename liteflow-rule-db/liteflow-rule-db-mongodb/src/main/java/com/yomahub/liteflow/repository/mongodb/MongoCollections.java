package com.yomahub.liteflow.repository.mongodb;

/** Collection layout for one MongoDB Rule-DB database. */
final class MongoCollections {

	private final String prefix;

	MongoCollections(String prefix) {
		this.prefix = MongoStorageValidator.collectionPrefixOrDefault(prefix);
	}

	String chains() { return prefix + "chain"; }
	String scripts() { return prefix + "script"; }
	String changes() { return prefix + "change_log"; }
	String sequences() { return prefix + "sequence"; }
}
