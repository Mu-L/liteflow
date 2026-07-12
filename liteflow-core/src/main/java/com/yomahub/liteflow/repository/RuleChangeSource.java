package com.yomahub.liteflow.repository;

/**
 * Backend-neutral stream of rule changes. Implementations use open/activate
 * to buffer events while the initial manifest is being loaded.
 */
public interface RuleChangeSource extends AutoCloseable {

	void open(RuleChangeListener listener);

	void activate(long baselineSeq);

	ChangeSourceHealth health();

	@Override
	default void close() {
	}
}
