package com.yomahub.liteflow.publisher.exception;

/** Invalid publisher configuration or ambiguous provider resolution. */
public class PublisherConfigurationException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	/**
	 * Creates the exception with a description of the configuration problem.
	 *
	 * @param message description of the configuration problem
	 */
	public PublisherConfigurationException(String message) {
		super(message);
	}

	/**
	 * Creates the exception with a description and the underlying cause.
	 *
	 * @param message description of the configuration problem
	 * @param cause underlying cause, may be {@code null}
	 */
	public PublisherConfigurationException(String message, Throwable cause) {
		super(message, cause);
	}
}
