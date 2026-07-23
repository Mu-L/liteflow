package com.yomahub.liteflow.repository.redis;

import com.yomahub.liteflow.exception.ConfigErrorException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RedisKeysTest {

	@Test
	void defaultsProduceLegacyLayout() {
		RedisKeys keys = new RedisKeys(null, null);
		assertEquals("lf:default:chain-ids", keys.chainIds());
		assertEquals("lf:default:script-ids", keys.scriptIds());
		assertEquals("lf:default:seq", keys.seq());
		assertEquals("lf:default:changelog", keys.changelog());
		assertEquals("lf:default:chain:c1", keys.chain("c1"));
		assertEquals("lf:default:script:s1", keys.script("s1"));
	}

	@Test
	void hashTagWrapsClusterSlot() {
		RedisKeys keys = new RedisKeys("p", "a", "tag");
		assertEquals("p:{tag}:a:chain-ids", keys.chainIds());
		assertEquals("p:{tag}:a:chain:c1", keys.chain("c1"));
	}

	@Test
	void blankHashTagFallsBackToPlainLayout() {
		RedisKeys keys = new RedisKeys("p", "a", " ");
		assertEquals("p:a:chain-ids", keys.chainIds());
	}

	@Test
	void separatorOrWhitespaceInKeyPartsIsRejected() {
		assertThrows(ConfigErrorException.class, () -> new RedisKeys("bad:prefix", "app"));
		assertThrows(ConfigErrorException.class, () -> new RedisKeys("bad prefix", "app"));
		assertThrows(ConfigErrorException.class, () -> new RedisKeys("p", "bad:app"));
		assertThrows(ConfigErrorException.class, () -> new RedisKeys("p", "bad app"));
		assertThrows(ConfigErrorException.class, () -> new RedisKeys("p", "a", "bad:tag"));
		assertThrows(ConfigErrorException.class, () -> new RedisKeys("p", "a", "bad tag"));
	}

	@Test
	void bracesInHashTagAreRejected() {
		assertThrows(ConfigErrorException.class, () -> new RedisKeys("p", "a", "bad{tag"));
		assertThrows(ConfigErrorException.class, () -> new RedisKeys("p", "a", "bad}tag"));
	}
}
