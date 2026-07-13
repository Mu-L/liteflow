package com.yomahub.liteflow.repository.zk;

import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.exception.ConfigErrorException;

final class ZkPaths {

	private final String base;

	ZkPaths(String rootPath, String applicationName) {
		if (StrUtil.isBlank(applicationName)) {
			throw new ConfigErrorException("rule-db zk applicationName must not be blank");
		}
		String root = StrUtil.isBlank(rootPath) ? "/liteflow" : rootPath.trim();
		String normalized = trimSlashes(root);
		this.base = (normalized.isEmpty() ? "" : "/" + normalized) + "/" + trimSlashes(applicationName.trim());
	}

	String chainMetaRoot() { return base + "/chains/meta"; }
	String chainContentRoot() { return base + "/chains/content"; }
	String scriptMetaRoot() { return base + "/scripts/meta"; }
	String scriptContentRoot() { return base + "/scripts/content"; }

	String chainMeta(String chainId) { return chainMetaRoot() + "/" + id(chainId); }
	String chainContent(String chainId) { return chainContentRoot() + "/" + id(chainId); }
	String scriptMeta(String nodeId) { return scriptMetaRoot() + "/" + id(nodeId); }
	String scriptContent(String nodeId) { return scriptContentRoot() + "/" + id(nodeId); }

	String idFrom(String root, String path) {
		String prefix = root + "/";
		if (path == null || !path.startsWith(prefix) || path.length() == prefix.length()) {
			throw new IllegalArgumentException("invalid ZooKeeper rule path: " + path);
		}
		String id = path.substring(prefix.length());
		if (id.indexOf('/') >= 0) {
			throw new IllegalArgumentException("nested ZooKeeper rule path: " + path);
		}
		return id;
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
		while (start < end && value.charAt(start) == '/') { start++; }
		while (end > start && value.charAt(end - 1) == '/') { end--; }
		return value.substring(start, end);
	}
}
