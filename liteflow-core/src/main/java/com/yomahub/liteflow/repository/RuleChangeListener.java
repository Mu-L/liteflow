package com.yomahub.liteflow.repository;

import com.yomahub.liteflow.repository.vo.ChangeRecord;

import java.util.List;

/**
 * Rule-DB change listener registered through
 * {@link RuleChangeSource#open(RuleChangeListener)}. Implementations may
 * deliver changes in batches and request a manifest reconciliation through
 * {@link #onReconcileRequired()} when an incremental stream cannot be trusted.
 *
 * @author Bryan.Zhang
 * @since 2.16.1
 */
@FunctionalInterface
public interface RuleChangeListener {

	void onChanges(List<ChangeRecord> changes);

	default void onReconcileRequired() {
	}

}
