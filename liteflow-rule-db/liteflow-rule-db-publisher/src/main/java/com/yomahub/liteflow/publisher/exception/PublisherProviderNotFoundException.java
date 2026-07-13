package com.yomahub.liteflow.publisher.exception;

/** No publisher provider supports the supplied typed configuration. */
public class PublisherProviderNotFoundException extends PublisherConfigurationException {

	public PublisherProviderNotFoundException(String message) {
		super(message);
	}
}
