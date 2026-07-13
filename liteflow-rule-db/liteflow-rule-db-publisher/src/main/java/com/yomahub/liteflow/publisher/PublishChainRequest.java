package com.yomahub.liteflow.publisher;

import com.yomahub.liteflow.publisher.exception.RuleValidationException;

/** Immutable request for creating or updating a chain. */
public final class PublishChainRequest {

	private final String chainId;
	private final String el;
	private final String route;
	private final String namespace;
	private final Long expectedVersion;

	private PublishChainRequest(Builder builder) {
		this.chainId = builder.chainId;
		this.el = builder.el;
		this.route = builder.route;
		this.namespace = builder.namespace;
		this.expectedVersion = builder.expectedVersion;
		validate();
	}

	public static Builder builder() {
		return new Builder();
	}

	public String getChainId() {
		return chainId;
	}

	public String getTargetId() {
		return chainId;
	}

	public String getEl() {
		return el;
	}

	public String getRoute() {
		return route;
	}

	public String getNamespace() {
		return namespace;
	}

	public Long getExpectedVersion() {
		return expectedVersion;
	}

	void validate() {
		if (isBlank(chainId)) {
			throw new RuleValidationException("chainId must not be blank");
		}
		if (isBlank(el)) {
			throw new RuleValidationException("chain EL must not be blank");
		}
		validateExpectedVersion(expectedVersion);
	}

	private static void validateExpectedVersion(Long version) {
		if (version != null && version < 0) {
			throw new RuleValidationException("expectedVersion must not be negative");
		}
	}

	private static boolean isBlank(String value) {
		return value == null || value.trim().isEmpty();
	}

	public static final class Builder {

		private String chainId;
		private String el;
		private String route;
		private String namespace;
		private Long expectedVersion;

		private Builder() {
		}

		public Builder chainId(String chainId) {
			this.chainId = chainId;
			return this;
		}

		public Builder el(String el) {
			this.el = el;
			return this;
		}

		public Builder route(String route) {
			this.route = route;
			return this;
		}

		public Builder namespace(String namespace) {
			this.namespace = namespace;
			return this;
		}

		public Builder expectedVersion(Long expectedVersion) {
			this.expectedVersion = expectedVersion;
			return this;
		}

		public PublishChainRequest build() {
			return new PublishChainRequest(this);
		}
	}
}
