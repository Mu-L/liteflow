package com.yomahub.liteflow.repository;

/**
 * Unified Rule-DB provider. A provider owns the repository and change source
 * lifecycle so they can share backend resources.
 */
public interface RuleDbProvider extends AutoCloseable {

	/** Stable backend name used by runtime diagnostics. */
	default String type() {
		return "unknown";
	}

	RuleRepository repository();

	RuleChangeSource changeSource();

	@Override
	default void close() {
	}
}
