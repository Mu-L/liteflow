package com.yomahub.liteflow.repository.zk;

import com.yomahub.liteflow.exception.ConfigErrorException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Path construction and validation rules. */
class ZkPathsValidationTest {

	@Test
	void blankApplicationNameDefaultsRootToLiteflow() {
		ZkPaths paths = new ZkPaths(" ", "app");
		assertEquals("/liteflow/app/chains/meta", paths.chainMetaRoot());
		assertEquals("/liteflow/app/scripts/content", paths.scriptContentRoot());
	}

	@Test
	void nestedRootPathIsKeptSegmentBySegment() {
		ZkPaths paths = new ZkPaths("//rules/engine//", "app");
		assertEquals("/rules/engine/app/chains/meta", paths.chainMetaRoot());
	}

	@Test
	void applicationNameRejectsIllegalValues() {
		assertThrows(ConfigErrorException.class, () -> new ZkPaths("/lf", " "));
		assertThrows(ConfigErrorException.class, () -> new ZkPaths("/lf", "a/b"));
		assertThrows(ConfigErrorException.class, () -> new ZkPaths("/lf", ".."));
		assertThrows(ConfigErrorException.class, () -> new ZkPaths("/lf", "a..b"));
		assertThrows(ConfigErrorException.class, () -> new ZkPaths("/lf", "ab"));
	}

	@Test
	void rootPathRejectsIllegalSegments() {
		assertThrows(ConfigErrorException.class, () -> new ZkPaths("/lf/../escape", "app"));
		assertThrows(ConfigErrorException.class, () -> new ZkPaths("/lf//gap", "app"));
		assertThrows(ConfigErrorException.class, () -> new ZkPaths("/lf/ab", "app"));
	}

	@Test
	void singleSlashRootResolvesToApplicationOnly() {
		ZkPaths paths = new ZkPaths("/", "app");
		assertEquals("/app/chains/meta/c1", paths.chainMeta("c1"));
	}

	@Test
	void idFromRejectsForeignAndNestedPaths() {
		ZkPaths paths = new ZkPaths("/lf", "app");
		String root = paths.chainMetaRoot();
		assertEquals("c1", paths.idFrom(root, root + "/c1"));
		assertThrows(IllegalArgumentException.class, () -> paths.idFrom(root, "/other/c1"));
		assertThrows(IllegalArgumentException.class, () -> paths.idFrom(root, root));
		assertThrows(IllegalArgumentException.class, () -> paths.idFrom(root, root + "/a/b"));
		assertThrows(IllegalArgumentException.class, () -> paths.idFrom(root, null));
	}

	@Test
	void ruleIdsRejectBlankAndSlashes() {
		ZkPaths paths = new ZkPaths("/lf", "app");
		assertThrows(IllegalArgumentException.class, () -> paths.chainMeta(" "));
		assertThrows(IllegalArgumentException.class, () -> paths.scriptContent("a/b"));
	}
}
