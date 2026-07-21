package com.yomahub.liteflow.repository.redis;

import cn.hutool.core.util.StrUtil;

import com.yomahub.liteflow.exception.ConfigErrorException;

/** Key layout for one Redis application namespace. */
public final class RedisKeys {

	private final String base;

	public RedisKeys(String keyPrefix, String applicationName) {
		this(keyPrefix, applicationName, null);
	}

	public RedisKeys(String keyPrefix, String applicationName, String keyHashTag) {
		String prefix = StrUtil.isBlank(keyPrefix) ? "lf" : keyPrefix;
		String app = StrUtil.isBlank(applicationName) ? "default" : applicationName;
		if (StrUtil.isBlank(keyHashTag)) {
			this.base = prefix + ":" + app;
			return;
		}
		String hashTag = keyHashTag.trim();
		if (hashTag.indexOf('{') >= 0 || hashTag.indexOf('}') >= 0) {
			throw new ConfigErrorException("rule-db redis key-hash-tag must not contain '{' or '}'");
		}
		this.base = prefix + ":{" + hashTag + "}:" + app;
	}

	public String chain(String chainId) {
		return base + ":chain:" + chainId;
	}

	public String script(String nodeId) {
		return base + ":script:" + nodeId;
	}

	public String chainIds() {
		return base + ":chain-ids";
	}

	public String scriptIds() {
		return base + ":script-ids";
	}

	public String seq() {
		return base + ":seq";
	}

	public String changelog() {
		return base + ":changelog";
	}
}
