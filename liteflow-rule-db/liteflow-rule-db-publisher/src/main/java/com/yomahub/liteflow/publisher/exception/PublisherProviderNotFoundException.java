package com.yomahub.liteflow.publisher.exception;

/** No publisher provider supports the supplied typed configuration. */
public class PublisherProviderNotFoundException extends PublisherConfigurationException {

	private static final long serialVersionUID = 1L;

	/**
	 * Creates the exception naming the unsupported backend and configuration.
	 *
	 * @param message description of the unsupported backend and configuration
	 */
	public PublisherProviderNotFoundException(String message) {
		super(message);
	}
}
