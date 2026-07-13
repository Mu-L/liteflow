package com.yomahub.liteflow.publisher;

import com.yomahub.liteflow.publisher.exception.RuleValidationException;

/** Immutable request for removing a chain or script. */
public final class RemoveRuleRequest {

	private final String targetId;
	private final Long expectedVersion;

	private RemoveRuleRequest(Builder builder) {
		this.targetId = builder.targetId;
		this.expectedVersion = builder.expectedVersion;
		validate();
	}

	public static Builder builder() {
		return new Builder();
	}

	public String getTargetId() {
		return targetId;
	}

	public Long getExpectedVersion() {
		return expectedVersion;
	}

	void validate() {
		if (targetId == null || targetId.trim().isEmpty()) {
			throw new RuleValidationException("targetId must not be blank");
		}
		if (expectedVersion != null && expectedVersion < 0) {
			throw new RuleValidationException("expectedVersion must not be negative");
		}
	}

	public static final class Builder {

		private String targetId;
		private Long expectedVersion;

		private Builder() {
		}

		public Builder targetId(String targetId) {
			this.targetId = targetId;
			return this;
		}

		public Builder expectedVersion(Long expectedVersion) {
			this.expectedVersion = expectedVersion;
			return this;
		}

		public RemoveRuleRequest build() {
			return new RemoveRuleRequest(this);
		}
	}
}
