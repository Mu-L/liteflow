package com.yomahub.liteflow.repository.redis;

import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.PublishScriptRequest;
import com.yomahub.liteflow.publisher.RemoveRuleRequest;
import com.yomahub.liteflow.publisher.exception.RuleValidationException;

/**
 * Validates publisher requests against the Redis key layout. Target IDs become
 * part of Redis keys, so they must not contain the {@code ':'} separator or
 * whitespace; length caps mirror the SQL backend for cross-backend portability.
 */
final class RedisStorageValidator {

	static final int MAX_TARGET_ID_LENGTH = 128;
	static final int MAX_NAMESPACE_LENGTH = 64;
	static final int MAX_SCRIPT_NAME_LENGTH = 128;
	static final int MAX_SCRIPT_TYPE_LENGTH = 32;
	static final int MAX_SCRIPT_LANGUAGE_LENGTH = 32;

	private RedisStorageValidator() {
	}

	static void validateChainRequest(PublishChainRequest request) {
		if (request == null) {
			throw new RuleValidationException("publish chain request must not be null");
		}
		requireTargetId("chainId", request.getChainId());
		requireLength("namespace", request.getNamespace(), MAX_NAMESPACE_LENGTH);
	}

	static void validateScriptRequest(PublishScriptRequest request) {
		if (request == null) {
			throw new RuleValidationException("publish script request must not be null");
		}
		requireTargetId("nodeId", request.getNodeId());
		requireLength("script name", request.getName(), MAX_SCRIPT_NAME_LENGTH);
		requireLength("script type", request.getType(), MAX_SCRIPT_TYPE_LENGTH);
		requireLength("script language", request.getLanguage(), MAX_SCRIPT_LANGUAGE_LENGTH);
	}

	static void validateRemoveRequest(RemoveRuleRequest request) {
		if (request == null) {
			throw new RuleValidationException("remove rule request must not be null");
		}
		requireTargetId("targetId", request.getTargetId());
	}

	private static void requireTargetId(String field, String targetId) {
		if (StrUtil.isBlank(targetId)) {
			throw new RuleValidationException("Redis " + field + " must not be blank");
		}
		requireLength(field, targetId, MAX_TARGET_ID_LENGTH);
		for (int i = 0; i < targetId.length(); i++) {
			char c = targetId.charAt(i);
			if (c == ':' || Character.isWhitespace(c)) {
				throw new RuleValidationException(
						"Redis " + field + " must not contain ':' or whitespace");
			}
		}
	}

	private static void requireLength(String field, String value, int maxLength) {
		if (value != null && value.codePointCount(0, value.length()) > maxLength) {
			throw new RuleValidationException("Redis " + field + " exceeds limit " + maxLength);
		}
	}
}
