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

	public static Builder builder() {
		return new Builder();
	}

	public String getNodeId() {
		return nodeId;
	}

	public String getTargetId() {
		return nodeId;
	}

	public String getScript() {
		return script;
	}

	public String getName() {
		return name;
	}

	public String getType() {
		return type;
	}

	public String getLanguage() {
		return language;
	}

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

	public static final class Builder {

		private String nodeId;
		private String script;
		private String name;
		private String type;
		private String language;
		private Long expectedVersion;

		private Builder() {
		}

		public Builder nodeId(String nodeId) {
			this.nodeId = nodeId;
			return this;
		}

		public Builder script(String script) {
			this.script = script;
			return this;
		}

		public Builder name(String name) {
			this.name = name;
			return this;
		}

		public Builder type(String type) {
			this.type = type;
			return this;
		}

		public Builder type(NodeTypeEnum type) {
			this.type = type == null ? null : type.getCode();
			return this;
		}

		public Builder language(String language) {
			this.language = language;
			return this;
		}

		public Builder expectedVersion(Long expectedVersion) {
			this.expectedVersion = expectedVersion;
			return this;
		}

		public PublishScriptRequest build() {
			return new PublishScriptRequest(this);
		}
	}
}
