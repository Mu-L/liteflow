package com.yomahub.liteflow.publisher;

import com.yomahub.liteflow.publisher.exception.PublisherConfigurationException;

/**
 * Service-provider contract implemented by each Rule-DB backend module. Implementations
 * are discovered through {@link java.util.ServiceLoader} and must be registered in
 * {@code META-INF/services/com.yomahub.liteflow.publisher.RulePublisherProvider} with a
 * public no-argument constructor.
 */
public interface RulePublisherProvider {

	/**
	 * Tells whether this provider can build a publisher for the supplied configuration,
	 * typically by matching {@link RulePublisherConfig#backend()}.
	 *
	 * @param config backend-specific typed configuration
	 * @return {@code true} if this provider supports the configuration
	 */
	boolean supports(RulePublisherConfig config);

	/**
	 * Builds a publisher for the supplied configuration. Only invoked when
	 * {@link #supports(RulePublisherConfig)} returned {@code true} for the same configuration.
	 *
	 * @param config backend-specific typed configuration
	 * @return a ready-to-use publisher, never {@code null}
	 * @throws PublisherConfigurationException if the configuration cannot be turned into a publisher
	 */
	RulePublisher create(RulePublisherConfig config);
}
