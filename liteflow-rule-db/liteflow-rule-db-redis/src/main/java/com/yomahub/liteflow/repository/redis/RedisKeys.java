package com.yomahub.liteflow.repository.redis;

import cn.hutool.core.util.StrUtil;

/** Key layout for one Redis application namespace. */
public final class RedisKeys {

	private final String base;

	public RedisKeys(String keyPrefix, String applicationName) {
		String prefix = StrUtil.isBlank(keyPrefix) ? "lf" : keyPrefix;
		String app = StrUtil.isBlank(applicationName) ? "default" : applicationName;
		this.base = prefix + ":" + app;
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
