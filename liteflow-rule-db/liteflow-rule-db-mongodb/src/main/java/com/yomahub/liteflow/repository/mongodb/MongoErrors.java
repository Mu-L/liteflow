package com.yomahub.liteflow.repository.mongodb;

import com.mongodb.MongoException;

/** Shared classification of MongoDB driver errors. */
final class MongoErrors {

	static final String REPLICA_SET_HINT = "; MongoDB Rule-DB requires a replica set or sharded cluster for transactions";

	private MongoErrors() { }

	static boolean isTransactionUnsupported(MongoException e) {
		String message = e.getMessage();
		return e.getCode() == 20 || (message != null && message.toLowerCase().contains("transaction numbers"));
	}
}
