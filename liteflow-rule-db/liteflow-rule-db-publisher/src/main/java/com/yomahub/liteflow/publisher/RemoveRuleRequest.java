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

	/**
	 * Returns a new builder for a removal request.
	 *
	 * @return new builder instance
	 */
	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Returns the identifier of the chain or script to remove.
	 *
	 * @return target identifier, never blank
	 */
	public String getTargetId() {
		return targetId;
	}

	/**
	 * Returns the expected stored version for optimistic concurrency control.
	 *
	 * @return expected version, or {@code null} to skip the version check
	 */
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

	/** Builder for {@link RemoveRuleRequest}. */
	public static final class Builder {

		private String targetId;
		private Long expectedVersion;

		private Builder() {
		}

		/**
		 * Sets the identifier of the chain or script to remove.
		 *
		 * @param targetId target identifier, must not be blank
		 * @return this builder
		 */
		public Builder targetId(String targetId) {
			this.targetId = targetId;
			return this;
		}

		/**
		 * Sets the expected stored version for optimistic concurrency control.
		 *
		 * @param expectedVersion expected version, or {@code null} to skip the version check
		 * @return this builder
		 */
		public Builder expectedVersion(Long expectedVersion) {
			this.expectedVersion = expectedVersion;
			return this;
		}

		/**
		 * Builds the immutable request, validating required fields.
		 *
		 * @return validated removal request
		 * @throws RuleValidationException if a required field is missing or invalid
		 */
		public RemoveRuleRequest build() {
			return new RemoveRuleRequest(this);
		}
	}
}
