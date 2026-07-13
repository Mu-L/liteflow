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

	public static Builder builder() {
		return new Builder();
	}

	public String getTargetId() {
		return targetId;
	}

	public ChangeRecord.TargetType getTargetType() {
		return targetType;
	}

	public ChangeRecord.Op getOperation() {
		return operation;
	}

	public long getVersion() {
		return version;
	}

	public long getSequence() {
		return sequence;
	}

	public static final class Builder {

		private String targetId;
		private ChangeRecord.TargetType targetType;
		private ChangeRecord.Op operation;
		private long version;
		private long sequence;

		private Builder() {
		}

		public Builder targetId(String targetId) {
			this.targetId = targetId;
			return this;
		}

		public Builder targetType(ChangeRecord.TargetType targetType) {
			this.targetType = targetType;
			return this;
		}

		public Builder operation(ChangeRecord.Op operation) {
			this.operation = operation;
			return this;
		}

		public Builder version(long version) {
			this.version = version;
			return this;
		}

		public Builder sequence(long sequence) {
			this.sequence = sequence;
			return this;
		}

		public PublishResult build() {
			return new PublishResult(this);
		}
	}
}
