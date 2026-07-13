package com.yomahub.liteflow.publisher.exception;

/** A publish request is missing required rule metadata or content. */
public class RuleValidationException extends RuntimeException {

	public RuleValidationException(String message) {
		super(message);
	}

	public RuleValidationException(String message, Throwable cause) {
		super(message, cause);
	}
}
