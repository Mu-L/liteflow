package com.yomahub.liteflow.repository.postgresql;

/** Indicates a PostgreSQL change-log row that cannot be decoded. */
final class PostgresqlChangeLogCorruptionException extends RuntimeException {

	PostgresqlChangeLogCorruptionException(String message, Throwable cause) {
		super(message, cause);
	}
}
