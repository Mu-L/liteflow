package com.yomahub.liteflow.publisher;

import com.yomahub.liteflow.enums.NodeTypeEnum;
import com.yomahub.liteflow.publisher.exception.RuleValidationException;

/** Immutable request for creating or updating a script. */
public final class PublishScriptRequest {

	private final String nodeId;
	private final String script;
	private final String name;
	private final String type;
	private final String language;
	private final Long expectedVersion;

	private PublishScriptRequest(Builder builder) {
		this.nodeId = builder.nodeId;
		this.script = builder.script;
		this.name = builder.name;
		this.type = builder.type;
		this.language = builder.language;
		this.expectedVersion = builder.expectedVersion;
		validate();
	}

	/**
	 * Returns a new builder for a script publication request.
	 *
	 * @return new builder instance
	 */
	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Returns the identifier of the script node to create or update.
	 *
	 * @return script node identifier, never blank
	 */
	public String getNodeId() {
		return nodeId;
	}

	/**
	 * Returns the backend record identifier, which is the script node identifier.
	 *
	 * @return script node identifier
	 */
	public String getTargetId() {
		return nodeId;
	}

	/**
	 * Returns the script source code.
	 *
	 * @return script source code, never blank
	 */
	public String getScript() {
		return script;
	}

	/**
	 * Returns the display name of the script node, if any.
	 *
	 * @return script node name or {@code null}
	 */
	public String getName() {
		return name;
	}

	/**
	 * Returns the script node type code, always one of the script node types.
	 *
	 * @return script node type code
	 * @see NodeTypeEnum#getCode()
	 */
	public String getType() {
		return type;
	}

	/**
	 * Returns the script language identifier, if any.
	 *
	 * @return script language or {@code null}
	 */
	public String getLanguage() {
		return language;
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
		if (isBlank(nodeId)) {
			throw new RuleValidationException("nodeId must not be blank");
		}
		if (isBlank(script)) {
			throw new RuleValidationException("script must not be blank");
		}
		NodeTypeEnum nodeType = NodeTypeEnum.getEnumByCode(type);
		if (nodeType == null || !nodeType.isScript()) {
			throw new RuleValidationException("script type must be a valid script node type");
		}
		if (expectedVersion != null && expectedVersion < 0) {
			throw new RuleValidationException("expectedVersion must not be negative");
		}
	}

	private static boolean isBlank(String value) {
		return value == null || value.trim().isEmpty();
	}

	/** Builder for {@link PublishScriptRequest}. */
	public static final class Builder {

		private String nodeId;
		private String script;
		private String name;
		private String type;
		private String language;
		private Long expectedVersion;

		private Builder() {
		}

		/**
		 * Sets the identifier of the script node to create or update.
		 *
		 * @param nodeId script node identifier, must not be blank
		 * @return this builder
		 */
		public Builder nodeId(String nodeId) {
			this.nodeId = nodeId;
			return this;
		}

		/**
		 * Sets the script source code.
		 *
		 * @param script script source code, must not be blank
		 * @return this builder
		 */
		public Builder script(String script) {
			this.script = script;
			return this;
		}

		/**
		 * Sets the display name of the script node.
		 *
		 * @param name script node name, may be {@code null}
		 * @return this builder
		 */
		public Builder name(String name) {
			this.name = name;
			return this;
		}

		/**
		 * Sets the script node type by its code.
		 *
		 * @param type script node type code, must resolve to a script node type
		 * @return this builder
		 */
		public Builder type(String type) {
			this.type = type;
			return this;
		}

		/**
		 * Sets the script node type by enum, using its code.
		 *
		 * @param type script node type, must be one of the script node types
		 * @return this builder
		 */
		public Builder type(NodeTypeEnum type) {
			this.type = type == null ? null : type.getCode();
			return this;
		}

		/**
		 * Sets the script language identifier.
		 *
		 * @param language script language, may be {@code null}
		 * @return this builder
		 */
		public Builder language(String language) {
			this.language = language;
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
		 * @return validated script publication request
		 * @throws RuleValidationException if a required field is missing or invalid
		 */
		public PublishScriptRequest build() {
			return new PublishScriptRequest(this);
		}
	}
}
