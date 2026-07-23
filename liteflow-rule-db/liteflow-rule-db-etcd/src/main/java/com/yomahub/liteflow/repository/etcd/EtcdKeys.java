package com.yomahub.liteflow.repository.etcd;

import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.exception.ConfigErrorException;

final class EtcdKeys {

	private final String base;

	EtcdKeys(String rootPath, String applicationName) {
		if (StrUtil.isBlank(applicationName)) {
			throw new ConfigErrorException("rule-db etcd applicationName must not be blank");
		}
		String root = StrUtil.isBlank(rootPath) ? "/liteflow" : rootPath.trim();
		root = "/" + trimSlashes(root);
		this.base = root + "/" + trimSlashes(applicationName.trim());
	}

	String rootPrefix() {
		return base + "/";
	}

	String chainMetaPrefix() {
		return base + "/chains/meta/";
	}

	String chainContentPrefix() {
		return base + "/chains/content/";
	}

	String scriptMetaPrefix() {
		return base + "/scripts/meta/";
	}

	String scriptContentPrefix() {
		return base + "/scripts/content/";
	}

	String chainMeta(String chainId) {
		return chainMetaPrefix() + id(chainId);
	}

	String chainContent(String chainId) {
		return chainContentPrefix() + id(chainId);
	}

	String scriptMeta(String nodeId) {
		return scriptMetaPrefix() + id(nodeId);
	}

	String scriptContent(String nodeId) {
		return scriptContentPrefix() + id(nodeId);
	}

	String idFrom(String prefix, String key) {
		if (key == null || !key.startsWith(prefix) || key.length() == prefix.length()) {
			throw new IllegalArgumentException("invalid etcd rule key: " + key);
		}
		return key.substring(prefix.length());
	}

	private String id(String value) {
		if (StrUtil.isBlank(value) || value.indexOf('/') >= 0) {
			throw new IllegalArgumentException("rule id must not be blank or contain '/'");
		}
		return value;
	}

	private static String trimSlashes(String value) {
		int start = 0;
		int end = value.length();
		while (start < end && value.charAt(start) == '/') {
			start++;
		}
		while (end > start && value.charAt(end - 1) == '/') {
			end--;
		}
		return value.substring(start, end);
	}
}
