/**
 * Backend-neutral API for atomically publishing LiteFlow rule records into a Rule-DB
 * backend.
 *
 * <p>Callers obtain a {@link com.yomahub.liteflow.publisher.RulePublisher} from
 * {@link com.yomahub.liteflow.publisher.RulePublisherFactory} using a backend-specific
 * {@link com.yomahub.liteflow.publisher.RulePublisherConfig}. Backend modules plug in
 * through the {@link com.yomahub.liteflow.publisher.RulePublisherProvider} service-provider
 * contract, discovered via {@link java.util.ServiceLoader}.
 */
package com.yomahub.liteflow.publisher;
