package com.yomahub.liteflow.repository;

/**
 * Optional deterministic polling seam for change sources used by tests and
 * legacy adapters. Backend implementations may instead schedule polling
 * internally and leave {@link RuleDbSyncManager#pollOnce()} as a no-op.
 */
public interface ManualPollingChangeSource {

	void pollOnce();
}
