package com.yomahub.liteflow.repository.runtime;

/** Lifecycle state of one Rule-DB chain or script target. */
public enum RuleTargetStatus {

	SHADOW,
	READY,
	STALE,
	LOADING,
	FAILED,
	DELETED
}
