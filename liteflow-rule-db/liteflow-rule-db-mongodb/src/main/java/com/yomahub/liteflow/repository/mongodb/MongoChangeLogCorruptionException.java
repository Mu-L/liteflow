package com.yomahub.liteflow.repository.mongodb;

/** Indicates a MongoDB change-log document that cannot be decoded. */
final class MongoChangeLogCorruptionException extends RuntimeException {

	MongoChangeLogCorruptionException(String message, Throwable cause) { super(message, cause); }
}
