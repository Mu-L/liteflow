package com.yomahub.liteflow.publisher;

import com.yomahub.liteflow.repository.vo.ChangeRecord;

/** Immutable result of a committed backend publication. */
public final class PublishResult {

	private final String targetId;
	private final ChangeRecord.TargetType targetType;
	private final ChangeRecord.Op operation;
	private final long version;
	private final long sequence;

	private PublishResult(Builder builder) {
		this.targetId = builder.targetId;
		this.targetType = builder.targetType;
		this.operation = builder.operation;
		this.version = builder.version;
		this.sequence = builder.sequence;
	}

	/**
	 * Returns a new builder for a publication result.
	 *
	 * @return new builder instance
	 */
	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Returns the identifier of the published chain or script.
	 *
	 * @return published target identifier
	 */
	public String getTargetId() {
		return targetId;
	}

	/**
	 * Returns the kind of the published record.
	 *
	 * @return published target type
	 */
	public ChangeRecord.TargetType getTargetType() {
		return targetType;
	}

	/**
	 * Returns the operation that was committed.
	 *
	 * @return committed operation
	 */
	public ChangeRecord.Op getOperation() {
		return operation;
	}

	/**
	 * Returns the business version of the record after the commit.
	 *
	 * @return committed business version
	 */
	public long getVersion() {
		return version;
	}

	/**
	 * Returns the backend change-log sequence of the commit.
	 *
	 * @return committed change-log sequence
	 */
	public long getSequence() {
		return sequence;
	}

	/** Builder for {@link PublishResult}. */
	public static final class Builder {

		private String targetId;
		private ChangeRecord.TargetType targetType;
		private ChangeRecord.Op operation;
		private long version;
		private long sequence;

		private Builder() {
		}

		/**
		 * Sets the identifier of the published chain or script.
		 *
		 * @param targetId published target identifier
		 * @return this builder
		 */
		public Builder targetId(String targetId) {
			this.targetId = targetId;
			return this;
		}

		/**
		 * Sets the kind of the published record.
		 *
		 * @param targetType published target type
		 * @return this builder
		 */
		public Builder targetType(ChangeRecord.TargetType targetType) {
			this.targetType = targetType;
			return this;
		}

		/**
		 * Sets the operation that was committed.
		 *
		 * @param operation committed operation
		 * @return this builder
		 */
		public Builder operation(ChangeRecord.Op operation) {
			this.operation = operation;
			return this;
		}

		/**
		 * Sets the business version of the record after the commit.
		 *
		 * @param version committed business version
		 * @return this builder
		 */
		public Builder version(long version) {
			this.version = version;
			return this;
		}

		/**
		 * Sets the backend change-log sequence of the commit.
		 *
		 * @param sequence committed change-log sequence
		 * @return this builder
		 */
		public Builder sequence(long sequence) {
			this.sequence = sequence;
			return this;
		}

		/**
		 * Builds the immutable result.
		 *
		 * @return publication result
		 */
		public PublishResult build() {
			if (targetId == null || targetId.trim().isEmpty()) {
				throw new IllegalStateException("publish result targetId must not be blank");
			}
			if (targetType == null) {
				throw new IllegalStateException("publish result targetType must not be null");
			}
			if (operation == null) {
				throw new IllegalStateException("publish result operation must not be null");
			}
			if (version < 0) {
				throw new IllegalStateException("publish result version must not be negative");
			}
			if (sequence <= 0) {
				throw new IllegalStateException("publish result sequence must be positive");
			}
			return new PublishResult(this);
		}
	}
}
