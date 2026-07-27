package com.yomahub.liteflow.repository.vo;

import com.yomahub.liteflow.repository.ChangeSourceHealth;
import com.yomahub.liteflow.repository.runtime.RuleTargetStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/** Immutable, content-free snapshot of the active Rule-DB runtime. */
public final class RuleDbRuntimeSnapshot {

	private final boolean active;
	private final String provider;
	private final ChangeSourceHealth changeSource;
	private final long lastAppliedSeq;
	private final long lastSuccessfulReconcileTime;
	private final String lastReconcileError;
	private final Map<RuleTargetStatus, Integer> targetCounts;
	private final List<FailedTarget> failedTargets;

	public RuleDbRuntimeSnapshot(boolean active, String provider, ChangeSourceHealth changeSource,
			long lastAppliedSeq, long lastSuccessfulReconcileTime, String lastReconcileError,
			Map<RuleTargetStatus, Integer> targetCounts, List<FailedTarget> failedTargets) {
		this.active = active;
		this.provider = provider;
		this.changeSource = changeSource;
		this.lastAppliedSeq = lastAppliedSeq;
		this.lastSuccessfulReconcileTime = lastSuccessfulReconcileTime;
		this.lastReconcileError = lastReconcileError;
		EnumMap<RuleTargetStatus, Integer> counts = new EnumMap<>(RuleTargetStatus.class);
		for (RuleTargetStatus status : RuleTargetStatus.values()) {
			counts.put(status, targetCounts == null ? 0 : targetCounts.getOrDefault(status, 0));
		}
		this.targetCounts = Collections.unmodifiableMap(counts);
		this.failedTargets = failedTargets == null ? Collections.emptyList()
				: Collections.unmodifiableList(new ArrayList<>(failedTargets));
	}

	public static RuleDbRuntimeSnapshot inactive() {
		return new RuleDbRuntimeSnapshot(false, null, null, 0L, 0L, null,
				Collections.emptyMap(), Collections.emptyList());
	}

	public boolean isActive() { return active; }
	public String getProvider() { return provider; }
	public ChangeSourceHealth getChangeSource() { return changeSource; }
	public long getLastAppliedSeq() { return lastAppliedSeq; }
	public long getLastSuccessfulReconcileTime() { return lastSuccessfulReconcileTime; }
	public String getLastReconcileError() { return lastReconcileError; }
	public Map<RuleTargetStatus, Integer> getTargetCounts() { return targetCounts; }
	public List<FailedTarget> getFailedTargets() { return failedTargets; }

	public static final class FailedTarget {

		private final ChangeRecord.TargetType targetType;
		private final String targetId;
		private final RuleTargetStatus status;
		private final long desiredVersion;
		private final long activeVersion;
		private final String error;

		public FailedTarget(ChangeRecord.TargetType targetType, String targetId, RuleTargetStatus status,
				long desiredVersion, long activeVersion, String error) {
			this.targetType = targetType;
			this.targetId = targetId;
			this.status = status;
			this.desiredVersion = desiredVersion;
			this.activeVersion = activeVersion;
			this.error = error;
		}

		public ChangeRecord.TargetType getTargetType() { return targetType; }
		public String getTargetId() { return targetId; }
		public RuleTargetStatus getStatus() { return status; }
		public long getDesiredVersion() { return desiredVersion; }
		public long getActiveVersion() { return activeVersion; }
		public String getError() { return error; }
	}
}
