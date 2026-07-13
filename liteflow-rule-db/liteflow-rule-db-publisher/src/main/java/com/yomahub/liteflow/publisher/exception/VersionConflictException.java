package com.yomahub.liteflow.publisher.exception;

/** The stored business version does not match the request expectation. */
public class VersionConflictException extends RuntimeException {

	public VersionConflictException(String message) {
		super(message);
	}

	public VersionConflictException(String message, Throwable cause) {
		super(message, cause);
	}
}
