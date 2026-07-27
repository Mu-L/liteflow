package com.yomahub.liteflow.repository.redis;

import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.property.RuleDbRedisConfig;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class RedisConnectionManagerTest {

	@Test
	void singleServerBranchNormalizesAddressAndAppliesCredentials() throws IOException {
		RedisConnectionManager manager = new RedisConnectionManager(publisherConfig()
				.address("127.0.0.1:6379").username("u").password("p").database(3).build());
		Config config = manager.buildConfig();
		assertFalse(config.isSentinelConfig());
		assertFalse(config.isClusterConfig());
		String json = config.toJSON();
		assertTrue(json.contains("redis://127.0.0.1:6379"), json);
		assertTrue(json.contains("\"database\":3"), json);
		assertTrue(json.contains("\"username\":\"u\""), json);
		assertTrue(json.contains("\"password\":\"p\""), json);
	}

	@Test
	void singleSentinelAddressWithMasterNameUsesSentinelMode() throws IOException {
		RedisConnectionManager manager = new RedisConnectionManager(publisherConfig()
				.address("redis://127.0.0.1:26379").masterName("mymaster").build());
		Config config = manager.buildConfig();
		assertTrue(config.isSentinelConfig());
		String json = config.toJSON();
		assertTrue(json.contains("\"masterName\":\"mymaster\""), json);
		assertTrue(json.contains("redis://127.0.0.1:26379"), json);
	}

	@Test
	void multipleAddressesWithMasterNameUseSentinelMode() throws IOException {
		RedisConnectionManager manager = new RedisConnectionManager(publisherConfig()
				.address("redis://127.0.0.1:26379,redis://127.0.0.1:26380")
				.masterName("mymaster").username("u").password("p").database(1).build());
		Config config = manager.buildConfig();
		assertTrue(config.isSentinelConfig());
		assertFalse(config.isClusterConfig());
		String json = config.toJSON();
		assertTrue(json.contains("redis://127.0.0.1:26379"), json);
		assertTrue(json.contains("redis://127.0.0.1:26380"), json);
		assertTrue(json.contains("\"username\":\"u\""), json);
		assertTrue(json.contains("\"database\":1"), json);
	}

	@Test
	void multipleAddressesWithoutMasterNameUseClusterModeWithHashTag() throws IOException {
		RedisConnectionManager manager = new RedisConnectionManager(publisherConfig()
				.address("redis://127.0.0.1:7000,redis://127.0.0.1:7001")
				.keyHashTag("lf").username("u").password("p").build());
		Config config = manager.buildConfig();
		assertTrue(config.isClusterConfig());
		String json = config.toJSON();
		assertTrue(json.contains("redis://127.0.0.1:7000"), json);
		assertTrue(json.contains("redis://127.0.0.1:7001"), json);
		assertTrue(json.contains("\"password\":\"p\""), json);
	}

	@Test
	void clusterModeWithoutHashTagFailsFast() {
		RedisConnectionManager manager = new RedisConnectionManager(publisherConfig()
				.address("redis://127.0.0.1:7000,redis://127.0.0.1:7001").build());
		ConfigErrorException error = assertThrows(ConfigErrorException.class, manager::buildConfig);
		assertTrue(error.getMessage().contains("key-hash-tag"));
	}

	@Test
	void executionConfigAlsoBuildsAllModes() {
		RuleDbRedisConfig single = new RuleDbRedisConfig();
		single.setAddress("127.0.0.1:6379");
		Config singleConfig = new RedisConnectionManager(single).buildConfig();
		assertFalse(singleConfig.isSentinelConfig());
		assertFalse(singleConfig.isClusterConfig());

		RuleDbRedisConfig sentinel = new RuleDbRedisConfig();
		sentinel.setAddress("127.0.0.1:26379");
		sentinel.setMasterName("mymaster");
		assertTrue(new RedisConnectionManager(sentinel).buildConfig().isSentinelConfig());
	}

	@Test
	void borrowedClusterClientWithoutHashTagFailsFast() {
		RedissonClient client = mock(RedissonClient.class);
		Config clusterConfig = new Config();
		clusterConfig.useClusterServers().addNodeAddress("redis://127.0.0.1:7000");
		doReturn(clusterConfig).when(client).getConfig();

		RedisPublisherConfig config = publisherConfig().redissonClient(client).build();
		ConfigErrorException error = assertThrows(ConfigErrorException.class,
				() -> new RedisConnectionManager(config));
		assertTrue(error.getMessage().contains("key-hash-tag"));

		RedisPublisherConfig withTag = publisherConfig().keyHashTag("lf").redissonClient(client).build();
		RedisConnectionManager manager = new RedisConnectionManager(withTag);
		assertSame(client, manager.getClient());
		manager.shutdown();
		verify(client, never()).shutdown();
	}

	@Test
	void borrowedNonClusterClientIsAccepted() {
		RedissonClient client = mock(RedissonClient.class);
		Config singleConfig = new Config();
		singleConfig.useSingleServer().setAddress("redis://127.0.0.1:6379");
		doReturn(singleConfig).when(client).getConfig();

		RedisConnectionManager manager = new RedisConnectionManager(
				publisherConfig().redissonClient(client).build());
		assertSame(client, manager.getClient());
	}

	@Test
	void borrowedClientWithUnreadableConfigIsAccepted() {
		RedissonClient client = mock(RedissonClient.class);
		doThrow(new RuntimeException("no config")).when(client).getConfig();

		RedisConnectionManager manager = new RedisConnectionManager(
				publisherConfig().redissonClient(client).build());
		assertSame(client, manager.getClient());
	}

	@Test
	void missingClientAndAddressIsConfigError() {
		RedisConnectionManager manager = new RedisConnectionManager(publisherConfig().build());
		assertThrows(ConfigErrorException.class, manager::getClient);
	}

	private RedisPublisherConfig.Builder publisherConfig() {
		return RedisPublisherConfig.builder().applicationName("app").keyPrefix("lf");
	}
}
