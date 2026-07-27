package com.yomahub.liteflow.publisher;

/** Marker contract implemented by backend-specific publisher configurations. */
public interface RulePublisherConfig {

	/**
	 * Returns the name of the publishing application, recorded as the origin of every
	 * committed change.
	 *
	 * @return publishing application name, never blank
	 */
	String applicationName();

	/**
	 * Returns the backend this configuration targets.
	 *
	 * @return target backend, never {@code null}
	 */
	PublisherBackend backend();
}
