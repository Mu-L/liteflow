package com.yomahub.liteflow.repository.etcd;

import com.yomahub.liteflow.publisher.exception.RuleStorageException;
import com.yomahub.liteflow.repository.vo.ChainRecord;
import com.yomahub.liteflow.repository.vo.ScriptMeta;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Codec validation and edge branches not exercised through the publisher. */
class EtcdRecordCodecTest {

	private final EtcdRecordCodec codec = new EtcdRecordCodec();

	@Test
	void rejectsNullAndMalformedJson() {
		assertThrows(RuleStorageException.class, () -> codec.enabled(null));
		assertThrows(RuleStorageException.class, () -> codec.version("not-json"));
		assertThrows(RuleStorageException.class, () -> codec.decodeChainMeta("c1", "{]"));
	}

	@Test
	void rejectsMetadataWithoutVersionOrMd5() {
		assertThrows(RuleStorageException.class,
				() -> codec.decodeChainMeta("c1", "{\"md5\":\"m\"}"));
		assertThrows(RuleStorageException.class,
				() -> codec.decodeChainMeta("c1", "{\"version\":0,\"md5\":\"m\"}"));
		assertThrows(RuleStorageException.class,
				() -> codec.decodeChainMeta("c1", "{\"version\":1}"));
	}

	@Test
	void rejectsScriptMetaWithoutType() {
		assertThrows(RuleStorageException.class,
				() -> codec.decodeScriptMeta("s1", "{\"version\":1,\"md5\":\"m\"}"));
	}

	@Test
	void rejectsContentWithoutBodyOrVersion() {
		String meta = "{\"version\":1,\"md5\":\"m\"}";
		assertThrows(RuleStorageException.class,
				() -> codec.decodeChain("c1", meta, "{\"version\":1}"));
		assertThrows(RuleStorageException.class,
				() -> codec.decodeChain("c1", meta, "{\"content\":\"x\"}"));
	}

	@Test
	void rejectsMetadataContentVersionMismatch() {
		assertThrows(RuleStorageException.class, () -> codec.decodeChain("c1",
				"{\"version\":2,\"md5\":\"m\"}", "{\"version\":1,\"content\":\"x\"}"));
		assertThrows(RuleStorageException.class, () -> codec.decodeScript("s1",
				"{\"version\":2,\"md5\":\"m\",\"type\":\"t\"}", "{\"version\":1,\"content\":\"x\"}"));
	}

	@Test
	void enabledDefaultsToTrueAndReadsExplicitFalse() {
		assertTrue(codec.enabled("{\"version\":1,\"md5\":\"m\"}"));
		assertFalse(codec.enabled("{\"version\":1,\"md5\":\"m\",\"enable\":false}"));
		assertEquals(0, codec.version("{\"md5\":\"m\"}"));
	}

	@Test
	void chainRoundTripKeepsNullableFieldsNull() {
		ChainRecord record = codec.decodeChain("c1",
				"{\"version\":3,\"md5\":\"m\",\"enable\":true}",
				"{\"version\":3,\"content\":\"THEN(a)\"}");
		assertEquals(3, record.getVersion());
		assertEquals("THEN(a)", record.getEl());
		assertNull(record.getRoute());
		assertNull(record.getNamespace());
	}

	@Test
	void scriptMetaReadsOptionalFields() {
		ScriptMeta meta = codec.decodeScriptMeta("s1",
				"{\"version\":2,\"md5\":\"m\",\"type\":\"t\",\"language\":\"groovy\",\"name\":\"n\"}");
		assertEquals(2, meta.getVersion());
		assertEquals("t", meta.getType());
		assertEquals("groovy", meta.getLanguage());
		assertEquals("n", meta.getName());
	}
}
