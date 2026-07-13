package com.yomahub.liteflow.publisher;

/** Marker contract implemented by backend-specific publisher configurations. */
public interface RulePublisherConfig {

	String applicationName();

	PublisherBackend backend();
}
