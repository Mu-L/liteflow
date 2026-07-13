package com.yomahub.liteflow.repository.etcd;

import com.yomahub.liteflow.publisher.exception.RuleStorageException;
import com.yomahub.liteflow.repository.vo.ChainRecord;
import com.yomahub.liteflow.repository.vo.ScriptRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EtcdRecordCodecTest {

	private final EtcdRecordCodec codec = new EtcdRecordCodec();

	@Test
	void roundTripsChainMetadataSeparatelyFromContent() {
		ChainRecord source = new ChainRecord();
		source.setChainId("c1");
		source.setVersion(2);
		source.setMd5("m2");
		source.setRoute("r");
		source.setNamespace("n");
		source.setEnable(true);

		ChainRecord decoded = codec.decodeChain("c1", codec.encodeChainMeta(source),
				codec.encodeContent(2, "THEN(a)"));

		assertEquals(2, decoded.getVersion());
		assertEquals("THEN(a)", decoded.getEl());
		assertEquals("r", decoded.getRoute());
		assertEquals("n", decoded.getNamespace());
	}

	@Test
	void roundTripsScriptMetadata() {
		ScriptRecord source = new ScriptRecord();
		source.setNodeId("s1");
		source.setVersion(1);
		source.setMd5("m1");
		source.setType("script");
		source.setLanguage("groovy");
		source.setName("script one");

		ScriptRecord decoded = codec.decodeScript("s1", codec.encodeScriptMeta(source),
				codec.encodeContent(1, "return 1"));
		assertEquals("script", decoded.getType());
		assertEquals("groovy", decoded.getLanguage());
		assertEquals("return 1", decoded.getScript());
	}

	@Test
	void rejectsMetadataContentVersionMismatch() {
		ChainRecord source = new ChainRecord();
		source.setVersion(2);
		source.setMd5("m2");
		assertThrows(RuleStorageException.class, () -> codec.decodeChain("c1",
				codec.encodeChainMeta(source), codec.encodeContent(1, "THEN(a)")));
	}
}
