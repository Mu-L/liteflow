package com.yomahub.liteflow.repository.redis;

import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.property.RuleDbRedisConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RedisClusterConfigurationTest {

	@Test
	void legacyKeyLayoutIsUnchangedWithoutHashTag() {
		RedisKeys keys = new RedisKeys("lf", "orders");

		assertEquals("lf:orders:chain:c1", keys.chain("c1"));
		assertEquals("lf:orders:chain-ids", keys.chainIds());
	}

	@Test
	void clusterKeysShareTheConfiguredHashTag() {
		RedisKeys keys = new RedisKeys("lf", "orders", "liteflow-orders");

		assertTrue(keys.chain("c1").contains("{liteflow-orders}"));
		assertTrue(keys.script("s1").contains("{liteflow-orders}"));
		assertTrue(keys.chainIds().contains("{liteflow-orders}"));
		assertTrue(keys.scriptIds().contains("{liteflow-orders}"));
		assertTrue(keys.seq().contains("{liteflow-orders}"));
		assertTrue(keys.changelog().contains("{liteflow-orders}"));
	}

	@Test
	void rejectsHashTagsThatWouldChangeRedisSlotParsing() {
		assertThrows(ConfigErrorException.class, () -> new RedisKeys("lf", "orders", "bad{tag"));
		assertThrows(ConfigErrorException.class, () -> new RedisKeys("lf", "orders", "bad}tag"));
	}

	@Test
	void configuredClusterRequiresHashTagBeforeConnecting() {
		RuleDbRedisConfig config = new RuleDbRedisConfig();
		config.setAddress("redis://127.0.0.1:7000,redis://127.0.0.1:7001");

		RedisConnectionManager connection = new RedisConnectionManager(config);
		assertThrows(ConfigErrorException.class, connection::getClient);
	}
}
