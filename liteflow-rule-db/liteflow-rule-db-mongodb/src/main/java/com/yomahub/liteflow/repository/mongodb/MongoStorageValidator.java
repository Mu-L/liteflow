package com.yomahub.liteflow.repository.mongodb;

import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.PublishScriptRequest;
import com.yomahub.liteflow.publisher.RemoveRuleRequest;
import com.yomahub.liteflow.publisher.exception.RuleValidationException;

/** Validates MongoDB names and Rule-DB identifiers. */
final class MongoStorageValidator {

	private MongoStorageValidator() { }

	static String applicationNameOrDefault(String value) {
		String resolved = StrUtil.isBlank(value) ? "default" : value;
		if (resolved.codePointCount(0, resolved.length()) > 128) {
			throw new ConfigErrorException("rule-db mongodb application-name exceeds limit 128");
		}
		return resolved;
	}

	static String databaseOrDefault(String value) {
		String resolved = StrUtil.isBlank(value) ? "liteflow" : value;
		if (!resolved.matches("[A-Za-z0-9_-]+")) {
			throw new ConfigErrorException("rule-db mongodb database must contain only ASCII letters, digits, underscore, or hyphen");
		}
		return resolved;
	}

	static String collectionPrefixOrDefault(String value) {
		String resolved = StrUtil.isBlank(value) ? "lf_" : value;
		if (!resolved.matches("[A-Za-z0-9_]+") || resolved.length() > 64) {
			throw new ConfigErrorException("rule-db mongodb collection-prefix is invalid");
		}
		return resolved;
	}

	static void validateTargetId(String field, String value) {
		if (StrUtil.isBlank(value)) { throw new ConfigErrorException("rule-db mongodb " + field + " must not be blank"); }
		if (value.codePointCount(0, value.length()) > 128) {
			throw new ConfigErrorException("rule-db mongodb " + field + " exceeds limit 128");
		}
	}

	static void validateChainRequest(PublishChainRequest request) {
		requireRequestLength("chainId", request.getChainId(), 128);
		requireRequestLength("namespace", request.getNamespace(), 128);
	}

	static void validateScriptRequest(PublishScriptRequest request) {
		requireRequestLength("nodeId", request.getNodeId(), 128);
		requireRequestLength("script name", request.getName(), 256);
		requireRequestLength("script type", request.getType(), 64);
		requireRequestLength("script language", request.getLanguage(), 64);
	}

	static void validateRemoveRequest(RemoveRuleRequest request) {
		requireRequestLength("targetId", request.getTargetId(), 128);
	}

	private static void requireRequestLength(String field, String value, int max) {
		if (value != null && value.codePointCount(0, value.length()) > max) {
			throw new RuleValidationException("MongoDB " + field + " exceeds limit " + max);
		}
	}
}
