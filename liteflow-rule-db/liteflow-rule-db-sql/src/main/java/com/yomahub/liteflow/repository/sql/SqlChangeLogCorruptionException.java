package com.yomahub.liteflow.repository.sql;

/** Indicates a change-log row that cannot be decoded according to the SQL protocol. */
final class SqlChangeLogCorruptionException extends RuntimeException {

	SqlChangeLogCorruptionException(String message, Throwable cause) {
		super(message, cause);
	}
}
