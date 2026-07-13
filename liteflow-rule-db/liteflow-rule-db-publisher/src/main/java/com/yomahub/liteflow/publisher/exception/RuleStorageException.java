package com.yomahub.liteflow.publisher.exception;

/** A backend failed to commit or read data required for publication. */
public class RuleStorageException extends RuntimeException {

	public RuleStorageException(String message) {
		super(message);
	}

	public RuleStorageException(String message, Throwable cause) {
		super(message, cause);
	}
}
