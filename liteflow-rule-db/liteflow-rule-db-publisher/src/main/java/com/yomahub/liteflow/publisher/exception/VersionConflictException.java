package com.yomahub.liteflow.publisher.exception;

/** The stored business version does not match the request expectation. */
public class VersionConflictException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	/**
	 * Creates the exception with a description of the conflicting version.
	 *
	 * @param message description of the conflicting version
	 */
	public VersionConflictException(String message) {
		super(message);
	}

	/**
	 * Creates the exception with a description and the underlying cause.
	 *
	 * @param message description of the conflicting version
	 * @param cause underlying cause, may be {@code null}
	 */
	public VersionConflictException(String message, Throwable cause) {
		super(message, cause);
	}
}
