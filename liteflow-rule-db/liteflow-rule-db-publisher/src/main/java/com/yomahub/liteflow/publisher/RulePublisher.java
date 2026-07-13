package com.yomahub.liteflow.publisher;

/** Backend-neutral API for atomically publishing Rule-DB records. */
public interface RulePublisher extends AutoCloseable {

	PublishResult publishChain(PublishChainRequest request);

	PublishResult publishScript(PublishScriptRequest request);

	PublishResult removeChain(RemoveRuleRequest request);

	PublishResult removeScript(RemoveRuleRequest request);

	@Override
	void close();
}
