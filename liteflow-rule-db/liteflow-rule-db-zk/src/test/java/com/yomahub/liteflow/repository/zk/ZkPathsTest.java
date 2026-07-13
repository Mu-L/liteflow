package com.yomahub.liteflow.repository.zk;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ZkPathsTest {

	@Test
	void separatesMetadataFromContent() {
		ZkPaths paths = new ZkPaths("/liteflow/", "order-app");
		assertEquals("/liteflow/order-app/chains/meta/c1", paths.chainMeta("c1"));
		assertEquals("/liteflow/order-app/chains/content/c1", paths.chainContent("c1"));
		assertEquals("/liteflow/order-app/scripts/meta", paths.scriptMetaRoot());
	}

	@Test
	void rejectsNestedIds() {
		ZkPaths paths = new ZkPaths("/liteflow", "app");
		assertThrows(IllegalArgumentException.class, () -> paths.scriptMeta("a/b"));
	}
}
