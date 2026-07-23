package com.yomahub.liteflow.publisher.exception;

/** A backend failed to commit or read data required for publication. */
public class RuleStorageException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	/**
	 * Creates the exception with a description of the storage failure.
	 *
	 * @param message description of the storage failure
	 */
	public RuleStorageException(String message) {
		super(message);
	}

	/**
	 * Creates the exception with a description and the underlying backend failure.
	 *
	 * @param message description of the storage failure
	 * @param cause underlying backend failure, may be {@code null}
	 */
	public RuleStorageException(String message, Throwable cause) {
		super(message, cause);
	}
}
