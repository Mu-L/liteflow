package com.yomahub.liteflow.repository.nacos;

import cn.hutool.crypto.SecureUtil;
import com.yomahub.liteflow.publisher.exception.RuleStorageException;
import com.yomahub.liteflow.repository.vo.ChainRecord;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NacosCatalogCodecTest {

	private final NacosCatalogCodec codec = new NacosCatalogCodec();

	@Test
	void roundTripsCanonicalCatalogAndNullAsEmpty() {
		assertEquals(0, codec.decode(null).sequence());
		ChainRecord record = new ChainRecord();
		record.setChainId("c1");
		record.setEl("THEN(a)");
		record.setRoute("AND(a)");
		record.setNamespace("ns");
		record.setVersion(1);
		record.setMd5(SecureUtil.md5(record.getEl()));
		record.setEnable(true);
		ChangeRecord change = new ChangeRecord(1, ChangeRecord.TargetType.CHAIN,
				"c1", ChangeRecord.Op.UPSERT, 1);

		NacosCatalog decoded = codec.decode(codec.encode(NacosCatalog.empty().withChain(record, change)));

		assertEquals(1, decoded.sequence());
		assertEquals("THEN(a)", decoded.chain("c1").getEl());
		assertEquals("AND(a)", decoded.chain("c1").getRoute());
		assertEquals("ns", decoded.chain("c1").getNamespace());
		assertEquals(1, decoded.manifest().getLatestSeq());
	}

	@Test
	void rejectsBlankCorruptMd5DuplicateAndInconsistentChange() {
		assertThrows(RuleStorageException.class, () -> codec.decode(" "));
		String md5 = SecureUtil.md5("THEN(a)");
		String base = "{\"schemaVersion\":1,\"sequence\":1,\"chains\":["
				+ "{\"chainId\":\"c1\",\"el\":\"THEN(a)\",\"version\":1,"
				+ "\"md5\":\"" + md5 + "\",\"enable\":true}],\"scripts\":[],"
				+ "\"lastChange\":{\"seq\":1,\"targetType\":\"CHAIN\","
				+ "\"targetId\":\"c1\",\"op\":\"UPSERT\",\"version\":1}}";

		assertThrows(RuleStorageException.class, () -> codec.decode(base.replace(md5, "broken")));
		assertThrows(RuleStorageException.class,
				() -> codec.decode(base.replace("\"sequence\":1", "\"sequence\":2")));
		String duplicate = base.replace("],\"scripts\"", ","
				+ "{\"chainId\":\"c1\",\"el\":\"THEN(a)\",\"version\":1,"
				+ "\"md5\":\"" + md5 + "\",\"enable\":true}],\"scripts\"");
		RuleStorageException error = assertThrows(RuleStorageException.class, () -> codec.decode(duplicate));
		assertTrue(error.getMessage().contains("duplicate chain"));
	}
}
