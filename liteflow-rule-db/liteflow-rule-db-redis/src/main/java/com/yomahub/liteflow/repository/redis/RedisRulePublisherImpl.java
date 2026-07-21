package com.yomahub.liteflow.repository.redis;

import cn.hutool.core.io.resource.ResourceUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.crypto.SecureUtil;
import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.PublishResult;
import com.yomahub.liteflow.publisher.PublishScriptRequest;
import com.yomahub.liteflow.publisher.RemoveRuleRequest;
import com.yomahub.liteflow.publisher.RulePublisher;
import com.yomahub.liteflow.publisher.exception.RuleStorageException;
import com.yomahub.liteflow.publisher.exception.VersionConflictException;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import org.redisson.api.RScript;
import org.redisson.client.codec.StringCodec;

import java.util.Arrays;
import java.util.List;

/** Redis Lua implementation of the backend-neutral publisher API. */
final class RedisRulePublisherImpl implements RulePublisher {

	private final RedisConnectionManager connectionManager;
	private final RedisKeys keys;
	private final String publishChainLua = ResourceUtil.readUtf8Str("lua/publish-chain.lua");
	private final String publishScriptLua = ResourceUtil.readUtf8Str("lua/publish-script.lua");
	private final String removeLua = ResourceUtil.readUtf8Str("lua/remove.lua");

	RedisRulePublisherImpl(RedisPublisherConfig config) {
		this.connectionManager = new RedisConnectionManager(config);
		this.keys = new RedisKeys(config.getKeyPrefix(), config.applicationName(), config.getKeyHashTag());
	}

	@Override
	public PublishResult publishChain(PublishChainRequest request) {
		return execute("publish chain", request.getChainId(), ChangeRecord.TargetType.CHAIN,
				ChangeRecord.Op.UPSERT, publishChainLua,
				Arrays.asList(keys.chain(request.getChainId()), keys.chainIds(), keys.seq(), keys.changelog()),
				request.getChainId(), request.getEl(), emptyIfNull(request.getRoute()),
				emptyIfNull(request.getNamespace()), SecureUtil.md5(request.getEl()),
				expectedArgument(request.getExpectedVersion()));
	}

	@Override
	public PublishResult publishScript(PublishScriptRequest request) {
		return execute("publish script", request.getNodeId(), ChangeRecord.TargetType.SCRIPT,
				ChangeRecord.Op.UPSERT, publishScriptLua,
				Arrays.asList(keys.script(request.getNodeId()), keys.scriptIds(), keys.seq(), keys.changelog()),
				request.getNodeId(), request.getScript(), emptyIfNull(request.getName()), request.getType(),
				emptyIfNull(request.getLanguage()), SecureUtil.md5(request.getScript()),
				expectedArgument(request.getExpectedVersion()));
	}

	@Override
	public PublishResult removeChain(RemoveRuleRequest request) {
		return remove(request, ChangeRecord.TargetType.CHAIN, keys.chain(request.getTargetId()), keys.chainIds());
	}

	@Override
	public PublishResult removeScript(RemoveRuleRequest request) {
		return remove(request, ChangeRecord.TargetType.SCRIPT, keys.script(request.getTargetId()), keys.scriptIds());
	}

	@Override
	public void close() {
		connectionManager.shutdown();
	}

	private PublishResult remove(RemoveRuleRequest request, ChangeRecord.TargetType targetType,
			String contentKey, String idSetKey) {
		return execute("remove " + targetType.name().toLowerCase(), request.getTargetId(), targetType,
				ChangeRecord.Op.DELETE, removeLua,
				Arrays.asList(contentKey, idSetKey, keys.seq(), keys.changelog()),
				targetType.name(), request.getTargetId(), expectedArgument(request.getExpectedVersion()));
	}

	private PublishResult execute(String operation, String targetId, ChangeRecord.TargetType targetType,
			ChangeRecord.Op changeOperation, String lua, List<Object> luaKeys, Object... arguments) {
		try {
			List<Object> result = script().eval(RScript.Mode.READ_WRITE, lua,
					RScript.ReturnType.MULTI, luaKeys, arguments);
			long version = number(result, 0);
			long value = number(result, 1);
			if (version < 0) {
				Long expected = expectedFrom(arguments);
				throw new VersionConflictException(targetType.name().toLowerCase() + "[" + targetId
						+ "] expected version[" + expected + "] but current version is[" + value + "]");
			}
			return PublishResult.builder()
					.targetId(targetId)
					.targetType(targetType)
					.operation(changeOperation)
					.version(version)
					.sequence(value)
					.build();
		}
		catch (VersionConflictException e) {
			throw e;
		}
		catch (RuntimeException e) {
			throw new RuleStorageException("Redis " + operation + " failed: " + e.getMessage(), e);
		}
	}

	private RScript script() {
		return connectionManager.getClient().getScript(StringCodec.INSTANCE);
	}

	private long number(List<Object> values, int index) {
		if (values == null || index >= values.size() || values.get(index) == null) {
			throw new RuleStorageException("Redis publisher returned an incomplete result");
		}
		Object value = values.get(index);
		return value instanceof Number ? ((Number) value).longValue() : Long.parseLong(value.toString());
	}

	private Long expectedFrom(Object[] arguments) {
		if (arguments == null || arguments.length == 0) {
			return null;
		}
		String value = String.valueOf(arguments[arguments.length - 1]);
		return StrUtil.isBlank(value) ? null : Long.parseLong(value);
	}

	private String expectedArgument(Long expectedVersion) {
		return expectedVersion == null ? "" : String.valueOf(expectedVersion);
	}

	private String emptyIfNull(String value) {
		return value == null ? "" : value;
	}
}
