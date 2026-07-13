package com.yomahub.liteflow.publisher.exception;

/** Invalid publisher configuration or ambiguous provider resolution. */
public class PublisherConfigurationException extends RuntimeException {

	public PublisherConfigurationException(String message) {
		super(message);
	}

	public PublisherConfigurationException(String message, Throwable cause) {
		super(message, cause);
	}
}
