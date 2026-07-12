package com.yomahub.liteflow.repository.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yomahub.liteflow.repository.vo.ChangeRecord;

/**
 * ChangeRecord ↔ JSON。字段：seq/targetType/targetId/op/version。
 *
 * <p>与 Task 3 的 Lua 发布端（{@code redis.call('PUBLISH', ch, cjson.encode(...))}）对齐：
 * cjson 产出 lower-camel 的 {@code {"seq":..,"targetType":"CHAIN","targetId":..,"op":"UPSERT","version":..}}，
 * 与 {@link ChangeRecord} 字段名逐一对应；枚举按名序列化，无需额外注解。
 *
 * @author Bryan.Zhang
 * @since 2.16.1
 */
public class ChangeCodec {

	private static final ObjectMapper MAPPER = new ObjectMapper();

	public static String toJson(ChangeRecord c) {
		try {
			return MAPPER.writeValueAsString(c);
		} catch (Exception e) {
			throw new RuntimeException("encode ChangeRecord failed: " + e.getMessage(), e);
		}
	}

	public static ChangeRecord fromJson(String json) {
		try {
			return MAPPER.readValue(json, ChangeRecord.class);
		} catch (Exception e) {
			throw new RuntimeException("decode ChangeRecord failed: " + json, e);
		}
	}

}
