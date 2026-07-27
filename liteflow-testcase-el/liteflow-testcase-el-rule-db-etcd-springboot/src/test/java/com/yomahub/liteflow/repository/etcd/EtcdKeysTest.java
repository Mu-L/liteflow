package com.yomahub.liteflow.repository.etcd;

import com.yomahub.liteflow.exception.ConfigErrorException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Key layout rules: normalization, defaults and id validation. */
class EtcdKeysTest {

	@Test
	void buildsHierarchicalKeysWithNormalizedRoot() {
		EtcdKeys keys = new EtcdKeys("///lf//", " /app/ ");
		assertEquals("/lf/app/", keys.rootPrefix());
		assertEquals("/lf/app/chains/meta/", keys.chainMetaPrefix());
		assertEquals("/lf/app/chains/content/c1", keys.chainContent("c1"));
		assertEquals("/lf/app/scripts/meta/s1", keys.scriptMeta("s1"));
		assertEquals("/lf/app/scripts/content/s1", keys.scriptContent("s1"));
	}

	@Test
	void defaultsRootPathToLiteflow() {
		EtcdKeys keys = new EtcdKeys(" ", "app");
		assertEquals("/liteflow/app/chains/meta/c1", keys.chainMeta("c1"));
	}

	@Test
	void rejectsBlankApplicationName() {
		assertThrows(ConfigErrorException.class, () -> new EtcdKeys("/lf", " "));
	}

	@Test
	void rejectsInvalidIds() {
		EtcdKeys keys = new EtcdKeys("/lf", "app");
		assertThrows(IllegalArgumentException.class, () -> keys.chainMeta(" "));
		assertThrows(IllegalArgumentException.class, () -> keys.chainMeta("a/b"));
	}

	@Test
	void idFromStripsPrefixAndRejectsForeignKeys() {
		EtcdKeys keys = new EtcdKeys("/lf", "app");
		assertEquals("c1", keys.idFrom(keys.chainMetaPrefix(), keys.chainMeta("c1")));
		assertThrows(IllegalArgumentException.class, () -> keys.idFrom(keys.chainMetaPrefix(), null));
		assertThrows(IllegalArgumentException.class,
				() -> keys.idFrom(keys.chainMetaPrefix(), "/other/c1"));
		assertThrows(IllegalArgumentException.class,
				() -> keys.idFrom(keys.chainMetaPrefix(), keys.chainMetaPrefix()));
	}
}
