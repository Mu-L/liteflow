package com.yomahub.liteflow.repository.redis;

import com.yomahub.liteflow.repository.vo.ChangeRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ChangeCodecTest {

	@Test
	void decodesLuaChangelogJson() {
		ChangeRecord record = ChangeCodec.fromJson(
				"{\"seq\":7,\"targetType\":\"SCRIPT\",\"targetId\":\"s1\",\"op\":\"DELETE\",\"version\":3}");
		assertEquals(7, record.getSeq());
		assertEquals(ChangeRecord.TargetType.SCRIPT, record.getTargetType());
		assertEquals("s1", record.getTargetId());
		assertEquals(ChangeRecord.Op.DELETE, record.getOp());
		assertEquals(3, record.getVersion());
	}

	@Test
	void invalidJsonRaisesRuntimeException() {
		assertThrows(RuntimeException.class, () -> ChangeCodec.fromJson("not-json"));
	}
}
