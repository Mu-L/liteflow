package com.yomahub.liteflow.repository.redis;

import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.PublishResult;
import com.yomahub.liteflow.publisher.PublishScriptRequest;
import com.yomahub.liteflow.publisher.RemoveRuleRequest;
import com.yomahub.liteflow.publisher.RulePublisher;
import com.yomahub.liteflow.publisher.exception.VersionConflictException;
import com.yomahub.liteflow.repository.ChangeSourceHealth;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import com.yomahub.liteflow.repository.vo.RuleManifest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** End-to-end Lua, repository, and polling coverage against Redis 7. */
@Testcontainers(disabledWithoutDocker = true)
class RedisContainerIntegrationTest {

	@Container
	private static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7"))
			.withExposedPorts(6379);

	private RedissonClient client;

	@BeforeEach
	void setUp() {
		Config config = new Config();
		config.useSingleServer().setAddress(address());
		client = Redisson.create(config);
		client.getKeys().flushall();
	}

	@AfterEach
	void tearDown() {
		if (client != null) { client.shutdown(); }
	}

	@Test
	void luaPublishingManifestPollingAndCasWorkAgainstRealRedis() {
		RedisPublisherConfig config = RedisPublisherConfig.builder().applicationName("it-app")
				.keyPrefix("lf").keyHashTag("rule-db").redissonClient(client).build();
		RedisKeys keys = new RedisKeys("lf", "it-app", "rule-db");
		RedisRuleRepository repository = new RedisRuleRepository(new RedisConnectionManager(config), keys);

		try (RulePublisher publisher = new RedisRulePublisherProvider().create(config)) {
			PublishResult created = publisher.publishChain(PublishChainRequest.builder()
					.chainId("c1").el("THEN(a)").expectedVersion(0L).build());
			PublishResult updated = publisher.publishChain(PublishChainRequest.builder()
					.chainId("c1").el("THEN(a, b)").expectedVersion(1L).build());
			publisher.publishScript(PublishScriptRequest.builder().nodeId("s1").script("return 1")
					.name("script").type("script").language("groovy").expectedVersion(0L).build());
			assertEquals(1, created.getVersion());
			assertEquals(2, updated.getVersion());
			assertThrows(VersionConflictException.class, () -> publisher.publishChain(
					PublishChainRequest.builder().chainId("c1").el("THEN(x)").expectedVersion(1L).build()));
		}

		RuleManifest manifest = repository.fetchManifest();
		assertEquals(1, manifest.getChains().size());
		assertEquals(1, manifest.getScripts().size());
		assertEquals(3, manifest.getLatestSeq());
		assertEquals("THEN(a, b)", repository.fetchChain("c1").getEl());
		assertEquals("return 1", repository.fetchScript("s1").getScript());

		List<ChangeRecord> delivered = new CopyOnWriteArrayList<>();
		RedisPollingChangeSource source = new RedisPollingChangeSource(repository, 3600, 2);
		source.open(delivered::addAll);
		source.activate(0);
		try {
			source.pollOnce();
			assertEquals(3, delivered.size());
			assertEquals(3, source.health().getCursor());
			assertEquals(ChangeSourceHealth.Status.UP, source.health().getStatus());
		}
		finally {
			source.close();
		}

		try (RulePublisher publisher = new RedisRulePublisherProvider().create(config)) {
			publisher.removeChain(RemoveRuleRequest.builder().targetId("c1").expectedVersion(2L).build());
			publisher.removeScript(RemoveRuleRequest.builder().targetId("s1").expectedVersion(1L).build());
		}
		assertNull(repository.fetchChain("c1"));
		assertNull(repository.fetchScript("s1"));
		assertEquals(5, repository.fetchLatestSeq());
		repository.close();
	}

	private String address() {
		return "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379);
	}
}
