package com.yomahub.liteflow.repository.etcd;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EtcdKeysTest {

	@Test
	void buildsIsolatedMetadataAndContentKeys() {
		EtcdKeys keys = new EtcdKeys("/liteflow/", "order-app");
		assertEquals("/liteflow/order-app/chains/meta/c1", keys.chainMeta("c1"));
		assertEquals("/liteflow/order-app/chains/content/c1", keys.chainContent("c1"));
		assertEquals("/liteflow/order-app/scripts/meta/s1", keys.scriptMeta("s1"));
		assertEquals("/liteflow/order-app/scripts/content/s1", keys.scriptContent("s1"));
	}

	@Test
	void rejectsIdsThatCanEscapeTheirPrefix() {
		EtcdKeys keys = new EtcdKeys("liteflow", "app");
		assertThrows(IllegalArgumentException.class, () -> keys.chainMeta("a/b"));
	}
}
