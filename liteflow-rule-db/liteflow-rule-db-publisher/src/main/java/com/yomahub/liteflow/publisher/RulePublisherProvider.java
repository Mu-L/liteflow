package com.yomahub.liteflow.publisher;

/** Service-provider contract implemented by each Rule-DB backend module. */
public interface RulePublisherProvider {

	boolean supports(RulePublisherConfig config);

	RulePublisher create(RulePublisherConfig config);
}
