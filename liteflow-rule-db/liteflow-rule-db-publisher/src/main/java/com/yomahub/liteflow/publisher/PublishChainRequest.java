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

	/**
	 * Returns a new builder for a chain publication request.
	 *
	 * @return new builder instance
	 */
	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Returns the identifier of the chain to create or update.
	 *
	 * @return chain identifier, never blank
	 */
	public String getChainId() {
		return chainId;
	}

	/**
	 * Returns the backend record identifier, which is the chain identifier.
	 *
	 * @return chain identifier
	 */
	public String getTargetId() {
		return chainId;
	}

	/**
	 * Returns the EL expression defining the chain.
	 *
	 * @return chain EL expression, never blank
	 */
	public String getEl() {
		return el;
	}

	/**
	 * Returns the route associated with the chain, if any.
	 *
	 * @return route or {@code null} when the chain has none
	 */
	public String getRoute() {
		return route;
	}

	/**
	 * Returns the namespace the chain belongs to, if any.
	 *
	 * @return namespace or {@code null} for the default namespace
	 */
	public String getNamespace() {
		return namespace;
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

	/** Builder for {@link PublishChainRequest}. */
	public static final class Builder {

		private String chainId;
		private String el;
		private String route;
		private String namespace;
		private Long expectedVersion;

		private Builder() {
		}

		/**
		 * Sets the identifier of the chain to create or update.
		 *
		 * @param chainId chain identifier, must not be blank
		 * @return this builder
		 */
		public Builder chainId(String chainId) {
			this.chainId = chainId;
			return this;
		}

		/**
		 * Sets the EL expression defining the chain.
		 *
		 * @param el chain EL expression, must not be blank
		 * @return this builder
		 */
		public Builder el(String el) {
			this.el = el;
			return this;
		}

		/**
		 * Sets the route associated with the chain.
		 *
		 * @param route route, may be {@code null}
		 * @return this builder
		 */
		public Builder route(String route) {
			this.route = route;
			return this;
		}

		/**
		 * Sets the namespace the chain belongs to.
		 *
		 * @param namespace namespace, may be {@code null} for the default namespace
		 * @return this builder
		 */
		public Builder namespace(String namespace) {
			this.namespace = namespace;
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
		 * @return validated chain publication request
		 * @throws RuleValidationException if a required field is missing or invalid
		 */
		public PublishChainRequest build() {
			return new PublishChainRequest(this);
		}
	}
}
