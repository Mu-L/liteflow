package com.yomahub.liteflow.repository.redis;

import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbConfig;

/**
 * Redis 键名拼装：{prefix}:{app}:xxx。
 *
 * @author Bryan.Zhang
 * @since 2.16.1
 */
public class RedisKeys {

	private static String prefix() {
		RuleDbConfig cfg = LiteflowConfigGetter.get().getRuleDb();
		String p = cfg == null ? null : cfg.getKeyPrefix();
		return StrUtil.isBlank(p) ? "lf" : p;
	}

	private static String app() {
		RuleDbConfig cfg = LiteflowConfigGetter.get().getRuleDb();
		String a = cfg == null ? null : cfg.getApplicationName();
		return StrUtil.isBlank(a) ? "default" : a;
	}

	private static String base() {
		return prefix() + ":" + app();
	}

	public static String chain(String chainId) {
		return base() + ":chain:" + chainId;
	}

	public static String script(String nodeId) {
		return base() + ":script:" + nodeId;
	}

	public static String chainIndex() {
		return base() + ":chain-index";
	}

	public static String scriptIndex() {
		return base() + ":script-index";
	}

	public static String seq() {
		return base() + ":seq";
	}

	public static String changelog() {
		return base() + ":changelog";
	}

	public static String notifyChannel() {
		return base() + ":notify";
	}
}
