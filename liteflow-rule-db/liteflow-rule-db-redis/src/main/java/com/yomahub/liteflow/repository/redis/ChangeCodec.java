package com.yomahub.liteflow.repository.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yomahub.liteflow.repository.vo.ChangeRecord;

/**
 * Decodes {@link ChangeRecord} entries from the JSON written to the changelog
 * ZSet by the Lua scripts. The Lua {@code cjson} output uses lower-camel field
 * names ({@code seq/targetType/targetId/op/version}) matching the
 * {@link ChangeRecord} properties, with enums serialized by name, so no extra
 * annotations are required.
 *
 * @author Bryan.Zhang
 * @since 2.16.1
 */
public class ChangeCodec {

	private static final ObjectMapper MAPPER = new ObjectMapper();

	public static ChangeRecord fromJson(String json) {
		try {
			return MAPPER.readValue(json, ChangeRecord.class);
		} catch (Exception e) {
			throw new RuntimeException("decode ChangeRecord failed: " + json, e);
		}
	}

}
