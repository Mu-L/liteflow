package com.yomahub.liteflow.publisher.exception;

/** A publish request is missing required rule metadata or content. */
public class RuleValidationException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	/**
	 * Creates the exception with a description of the invalid field.
	 *
	 * @param message description of the invalid field
	 */
	public RuleValidationException(String message) {
		super(message);
	}

	/**
	 * Creates the exception with a description and the underlying cause.
	 *
	 * @param message description of the invalid field
	 * @param cause underlying cause, may be {@code null}
	 */
	public RuleValidationException(String message, Throwable cause) {
		super(message, cause);
	}
}
