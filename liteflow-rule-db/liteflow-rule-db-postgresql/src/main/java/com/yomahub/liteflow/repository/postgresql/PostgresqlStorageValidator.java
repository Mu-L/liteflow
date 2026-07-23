package com.yomahub.liteflow.repository.postgresql;

import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.PublishScriptRequest;
import com.yomahub.liteflow.publisher.RemoveRuleRequest;
import com.yomahub.liteflow.publisher.exception.RuleValidationException;

/** Validates values against the PostgreSQL schema shipped with this module. */
final class PostgresqlStorageValidator {

	private PostgresqlStorageValidator() { }

	static String applicationNameOrDefault(String value) {
		String resolved = StrUtil.isBlank(value) ? "default" : value;
		validateApplicationName(resolved);
		return resolved;
	}

	static void validateApplicationName(String value) {
		if (StrUtil.isBlank(value)) { throw new ConfigErrorException("rule-db postgresql application-name must not be blank"); }
		requireConfigLength("application-name", value, 64);
	}

	static void validateTablePrefix(String value) {
		if (StrUtil.isBlank(value)) { return; }
		if (!value.matches("[A-Za-z0-9_]+")) {
			throw new ConfigErrorException("rule-db postgresql table-prefix must contain only ASCII letters, digits, or underscore");
		}
		// PostgreSQL identifiers are limited to 63 bytes; idx_app_seq is the longest suffix.
		requireConfigLength("table-prefix", value, 52);
	}

	static void validateTargetId(String field, String value) {
		if (StrUtil.isBlank(value)) { throw new ConfigErrorException("rule-db postgresql " + field + " must not be blank"); }
		requireConfigLength(field, value, 128);
	}

	static void validateChainRequest(PublishChainRequest request) {
		requireRequestLength("chainId", request.getChainId(), 128);
		requireRequestLength("namespace", request.getNamespace(), 64);
	}

	static void validateScriptRequest(PublishScriptRequest request) {
		requireRequestLength("nodeId", request.getNodeId(), 128);
		requireRequestLength("script name", request.getName(), 128);
		requireRequestLength("script type", request.getType(), 32);
		requireRequestLength("script language", request.getLanguage(), 32);
	}

	static void validateRemoveRequest(RemoveRuleRequest request) {
		requireRequestLength("targetId", request.getTargetId(), 128);
	}

	private static void requireConfigLength(String field, String value, int max) {
		if (value != null && value.codePointCount(0, value.length()) > max) {
			throw new ConfigErrorException("rule-db postgresql " + field + " exceeds schema limit " + max);
		}
	}

	private static void requireRequestLength(String field, String value, int max) {
		if (value != null && value.codePointCount(0, value.length()) > max) {
			throw new RuleValidationException("PostgreSQL " + field + " exceeds schema limit " + max);
		}
	}
}
