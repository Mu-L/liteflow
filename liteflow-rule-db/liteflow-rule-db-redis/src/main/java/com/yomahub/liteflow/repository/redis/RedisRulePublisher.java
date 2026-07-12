package com.yomahub.liteflow.repository.redis;

import cn.hutool.core.io.resource.ResourceUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.crypto.SecureUtil;
import com.yomahub.liteflow.repository.vo.ScriptRecord;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

import java.util.Arrays;
import java.util.List;

/**
 * Redis 发布 API：Lua 原子完成 HSET 内容 + HSET index + INCR seq + ZADD changelog + PUBLISH。
 * 可独立于 FlowExecutor 使用。
 *
 * @author Bryan.Zhang
 * @since 2.16.1
 */
public class RedisRulePublisher {

	private final RedisConnectionManager connectionManager = new RedisConnectionManager();

	private final String publishChainLua = ResourceUtil.readUtf8Str("lua/publish-chain.lua");

	private final String publishScriptLua = ResourceUtil.readUtf8Str("lua/publish-script.lua");

	private final String removeLua = ResourceUtil.readUtf8Str("lua/remove.lua");

	private RScript script() {
		RedissonClient client = connectionManager.getClient();
		return client.getScript(StringCodec.INSTANCE);
	}

	public long publishChain(String chainId, String el) {
		return publishChain(chainId, el, "", "");
	}

	public long publishChain(String chainId, String el, String route, String namespace) {
		List<Object> keys = Arrays.asList(RedisKeys.chain(chainId), RedisKeys.chainIndex(), RedisKeys.seq(),
				RedisKeys.changelog(), RedisKeys.notifyChannel());
		Long version = script().eval(RScript.Mode.READ_WRITE, publishChainLua, RScript.ReturnType.INTEGER, keys,
				chainId, el, nz(route), nz(namespace), SecureUtil.md5(el));
		return version;
	}

	public long publishScript(ScriptRecord s) {
		List<Object> keys = Arrays.asList(RedisKeys.script(s.getNodeId()), RedisKeys.scriptIndex(), RedisKeys.seq(),
				RedisKeys.changelog(), RedisKeys.notifyChannel());
		Long version = script().eval(RScript.Mode.READ_WRITE, publishScriptLua, RScript.ReturnType.INTEGER, keys,
				s.getNodeId(), s.getScript(), nz(s.getName()), nz(s.getType()), nz(s.getLanguage()),
				SecureUtil.md5(s.getScript()));
		return version;
	}

	public void removeChain(String chainId) {
		List<Object> keys = Arrays.asList(RedisKeys.chain(chainId), RedisKeys.chainIndex(), RedisKeys.seq(),
				RedisKeys.changelog(), RedisKeys.notifyChannel());
		script().eval(RScript.Mode.READ_WRITE, removeLua, RScript.ReturnType.INTEGER, keys, "CHAIN", chainId);
	}

	public void removeScript(String nodeId) {
		List<Object> keys = Arrays.asList(RedisKeys.script(nodeId), RedisKeys.scriptIndex(), RedisKeys.seq(),
				RedisKeys.changelog(), RedisKeys.notifyChannel());
		script().eval(RScript.Mode.READ_WRITE, removeLua, RScript.ReturnType.INTEGER, keys, "SCRIPT", nodeId);
	}

	private static String nz(String s) {
		return StrUtil.isBlank(s) ? "" : s;
	}

}
