package com.yomahub.liteflow.repository.sql;

import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.enums.NodeTypeEnum;
import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.PublishScriptRequest;
import com.yomahub.liteflow.publisher.RemoveRuleRequest;
import com.yomahub.liteflow.publisher.exception.RuleValidationException;
import com.yomahub.liteflow.repository.vo.ScriptRecord;

import java.nio.charset.StandardCharsets;

/** Validates values against the fixed SQL schema shipped with this module. */
final class SqlStorageValidator {

	static final int MAX_TABLE_PREFIX_LENGTH = 54;
	static final int MAX_APPLICATION_NAME_LENGTH = 64;
	static final int MAX_TARGET_ID_LENGTH = 128;
	static final int MAX_NAMESPACE_LENGTH = 64;
	static final int MAX_SCRIPT_NAME_LENGTH = 128;
	static final int MAX_SCRIPT_TYPE_LENGTH = 32;
	static final int MAX_SCRIPT_LANGUAGE_LENGTH = 32;
	static final int MAX_TEXT_BYTES = 65535;

	private SqlStorageValidator() {
	}

	static String applicationNameOrDefault(String applicationName) {
		String resolved = StrUtil.isBlank(applicationName) ? "default" : applicationName;
		validateApplicationName(resolved);
		return resolved;
	}

	static void validateApplicationName(String applicationName) {
		if (StrUtil.isBlank(applicationName)) {
			throw new ConfigErrorException("rule-db sql application-name must not be blank");
		}
		requireLength("application-name", applicationName, MAX_APPLICATION_NAME_LENGTH);
	}

	static void validateTablePrefix(String tablePrefix) {
		if (StrUtil.isBlank(tablePrefix)) {
			return;
		}
		if (!tablePrefix.matches("[A-Za-z0-9_]+")) {
			throw new ConfigErrorException(
					"rule-db sql table-prefix must contain only ASCII letters, digits, or underscore");
		}
		requireLength("table-prefix", tablePrefix, MAX_TABLE_PREFIX_LENGTH);
	}

	static void validateChainRequest(PublishChainRequest request) {
		requireRequestLength("chainId", request.getChainId(), MAX_TARGET_ID_LENGTH);
		requireRequestLength("namespace", request.getNamespace(), MAX_NAMESPACE_LENGTH);
		requireRequestText("chain EL", request.getEl());
		requireRequestText("chain route", request.getRoute());
	}

	static void validateScriptRequest(PublishScriptRequest request) {
		requireRequestLength("nodeId", request.getNodeId(), MAX_TARGET_ID_LENGTH);
		requireRequestLength("script name", request.getName(), MAX_SCRIPT_NAME_LENGTH);
		requireRequestLength("script type", request.getType(), MAX_SCRIPT_TYPE_LENGTH);
		requireRequestLength("script language", request.getLanguage(), MAX_SCRIPT_LANGUAGE_LENGTH);
		requireRequestText("script", request.getScript());
	}

	static void validateRemoveRequest(RemoveRuleRequest request) {
		requireRequestLength("targetId", request.getTargetId(), MAX_TARGET_ID_LENGTH);
	}

	static void validateLegacyChain(String chainId, String el) {
		if (StrUtil.isBlank(chainId)) {
			throw new RuleValidationException("chainId must not be blank");
		}
		if (StrUtil.isBlank(el)) {
			throw new RuleValidationException("chain EL must not be blank");
		}
		requireRequestLength("chainId", chainId, MAX_TARGET_ID_LENGTH);
		requireRequestText("chain EL", el);
	}

	static void validateLegacyScript(ScriptRecord script) {
		if (script == null) {
			throw new RuleValidationException("script record must not be null");
		}
		if (StrUtil.isBlank(script.getNodeId())) {
			throw new RuleValidationException("nodeId must not be blank");
		}
		if (StrUtil.isBlank(script.getScript())) {
			throw new RuleValidationException("script must not be blank");
		}
		NodeTypeEnum nodeType = NodeTypeEnum.getEnumByCode(script.getType());
		if (nodeType == null || !nodeType.isScript()) {
			throw new RuleValidationException("script type must be a valid script node type");
		}
		requireRequestLength("nodeId", script.getNodeId(), MAX_TARGET_ID_LENGTH);
		requireRequestLength("script name", script.getName(), MAX_SCRIPT_NAME_LENGTH);
		requireRequestLength("script type", script.getType(), MAX_SCRIPT_TYPE_LENGTH);
		requireRequestLength("script language", script.getLanguage(), MAX_SCRIPT_LANGUAGE_LENGTH);
		requireRequestText("script", script.getScript());
	}

	static void validateTargetId(String field, String targetId) {
		if (StrUtil.isBlank(targetId)) {
			throw new ConfigErrorException("rule-db sql " + field + " must not be blank");
		}
		requireLength(field, targetId, MAX_TARGET_ID_LENGTH);
	}

	private static void requireLength(String field, String value, int maxLength) {
		if (value != null && value.codePointCount(0, value.length()) > maxLength) {
			throw new ConfigErrorException("rule-db sql " + field + " exceeds schema limit " + maxLength);
		}
	}

	private static void requireRequestLength(String field, String value, int maxLength) {
		if (value != null && value.codePointCount(0, value.length()) > maxLength) {
			throw new RuleValidationException("SQL " + field + " exceeds schema limit " + maxLength);
		}
	}

	private static void requireRequestText(String field, String value) {
		if (value != null && value.getBytes(StandardCharsets.UTF_8).length > MAX_TEXT_BYTES) {
			throw new RuleValidationException("SQL " + field + " exceeds TEXT limit " + MAX_TEXT_BYTES
					+ " UTF-8 bytes");
		}
	}
}
