package com.yomahub.liteflow.repository.redis;

import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.PublishResult;
import com.yomahub.liteflow.publisher.PublishScriptRequest;
import com.yomahub.liteflow.publisher.RemoveRuleRequest;
import com.yomahub.liteflow.publisher.RulePublisher;
import com.yomahub.liteflow.publisher.RulePublisherFactory;
import com.yomahub.liteflow.publisher.exception.VersionConflictException;
import com.yomahub.liteflow.repository.RuleChangeListener;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import com.yomahub.liteflow.repository.vo.ScriptMeta;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import redis.embedded.RedisServer;
import redis.embedded.core.ExecutableProvider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RedisPublisherContractTest {

	private static final int REDIS_PORT = 16380;
	private static final String APPLICATION_NAME = "redis-publisher-contract";

	private static RedisServer redisServer;
	private static RedissonClient client;

	private final RedisKeys keys = new RedisKeys("lf", APPLICATION_NAME);

	private RedisRuleRepository repository;
	private RulePublisher publisher;

	@BeforeAll
	public static void startRedis() throws Exception {
		redisServer = RedisServer.newRedisServer().port(REDIS_PORT).build();
		try {
			redisServer.start();
		}
		catch (Exception e) {
			try {
				redisServer.stop();
			}
			catch (Exception ignored) {
			}
			redisServer = RedisServer.newRedisServer()
					.executableProvider(ExecutableProvider.newExecutableInPath("redis-server"))
					.port(REDIS_PORT)
					.build();
			redisServer.start();
		}
		Config config = new Config();
		config.useSingleServer().setAddress("redis://127.0.0.1:" + REDIS_PORT);
		client = Redisson.create(config);
		client.getKeys().flushall();
	}

	@AfterAll
	public static void stopRedis() throws Exception {
		if (client != null) {
			client.shutdown();
		}
		if (redisServer != null) {
			redisServer.stop();
		}
	}

	@BeforeEach
	public void setUp() {
		RedisPublisherConfig config = RedisPublisherConfig.builder()
				.applicationName(APPLICATION_NAME)
				.redissonClient(client)
				.build();
		publisher = RulePublisherFactory.create(config);
		repository = new RedisRuleRepository(new RedisConnectionManager(config), keys);
	}

	@AfterEach
	public void tearDown() {
		publisher.close();
		repository.close();
	}

	@Test
	public void testCreateExactUpdateAndConflictLeavesAllStructuresUnchanged() {
		PublishResult created = publisher.publishChain(chain("casRedisChain", "THEN(a)", 0L));
		Assertions.assertEquals(1L, created.getVersion());

		Map<String, String> hashBefore = new HashMap<>(client.<String, String>getMap(
				keys.chain("casRedisChain"), StringCodec.INSTANCE).readAllMap());
		Set<String> idsBefore = new HashSet<>(client.<String>getSet(
				keys.chainIds(), StringCodec.INSTANCE).readAll());
		long sequenceBefore = repository.fetchLatestSeq();
		int logSizeBefore = client.getScoredSortedSet(keys.changelog(), StringCodec.INSTANCE).size();

		Assertions.assertThrows(VersionConflictException.class,
				() -> publisher.publishChain(chain("casRedisChain", "THEN(b)", 9L)));
		Assertions.assertEquals(hashBefore, client.<String, String>getMap(
				keys.chain("casRedisChain"), StringCodec.INSTANCE).readAllMap());
		Assertions.assertEquals(idsBefore, client.<String>getSet(keys.chainIds(), StringCodec.INSTANCE).readAll());
		Assertions.assertEquals(sequenceBefore, repository.fetchLatestSeq());
		Assertions.assertEquals(logSizeBefore,
				client.getScoredSortedSet(keys.changelog(), StringCodec.INSTANCE).size());

		PublishResult updated = publisher.publishChain(PublishChainRequest.builder()
				.chainId("casRedisChain")
				.el("THEN(b)")
				.route("route-b")
				.namespace("ns-b")
				.expectedVersion(created.getVersion())
				.build());
		Assertions.assertEquals(2L, updated.getVersion());
		Assertions.assertEquals("route-b", repository.fetchChain("casRedisChain").getRoute());
		Assertions.assertEquals("ns-b", repository.fetchChain("casRedisChain").getNamespace());
	}

	@Test
	public void testScriptMetadataAndExactUpdate() {
		PublishResult created = publisher.publishScript(PublishScriptRequest.builder()
				.nodeId("casRedisScript")
				.script("return true")
				.name("first")
				.type("boolean_script")
				.language("groovy")
				.expectedVersion(0L)
				.build());
		ScriptMeta first = repository.fetchScriptMeta("casRedisScript");
		Assertions.assertEquals(created.getVersion(), first.getVersion());
		Assertions.assertEquals("boolean_script", first.getType());
		Assertions.assertEquals("groovy", first.getLanguage());
		Assertions.assertEquals("first", first.getName());

		publisher.publishScript(PublishScriptRequest.builder()
				.nodeId("casRedisScript")
				.script("return 'a'")
				.name("second")
				.type("switch_script")
				.language("groovy")
				.expectedVersion(created.getVersion())
				.build());
		ScriptMeta second = repository.fetchScriptMeta("casRedisScript");
		Assertions.assertEquals("switch_script", second.getType());
		Assertions.assertEquals("second", second.getName());
	}

	@Test
	public void testDeleteConflictDoesNotDeleteOrAppendChange() {
		PublishResult created = publisher.publishChain(chain("casRedisDelete", "THEN(a)", 0L));
		long sequenceBefore = repository.fetchLatestSeq();

		Assertions.assertThrows(VersionConflictException.class,
				() -> publisher.removeChain(RemoveRuleRequest.builder()
						.targetId("casRedisDelete").expectedVersion(99L).build()));
		Assertions.assertNotNull(repository.fetchChain("casRedisDelete"));
		Assertions.assertEquals(sequenceBefore, repository.fetchLatestSeq());

		PublishResult removed = publisher.removeChain(RemoveRuleRequest.builder()
				.targetId("casRedisDelete").expectedVersion(created.getVersion()).build());
		Assertions.assertEquals(ChangeRecord.Op.DELETE, removed.getOperation());
		Assertions.assertNull(repository.fetchChain("casRedisDelete"));
		Assertions.assertFalse(client.<String>getSet(keys.chainIds(), StringCodec.INSTANCE)
				.contains("casRedisDelete"));
		Assertions.assertEquals(sequenceBefore + 1, removed.getSequence());
	}

	@Test
	public void testPollingSourceDeliversCompleteBatch() {
		long baseline = repository.fetchLatestSeq();
		RedisPollingChangeSource source = new RedisPollingChangeSource(repository, 3600);
		List<ChangeRecord> received = new ArrayList<>();
		source.open(new RuleChangeListener() {
			@Override
			public void onChanges(List<ChangeRecord> changes) {
				received.addAll(changes);
			}
		});
		source.activate(baseline);
		try {
			publisher.publishChain(chain("pollRedisChain", "THEN(a)", 0L));
			source.pollOnce();
			Assertions.assertEquals(1, received.size());
			Assertions.assertEquals("pollRedisChain", received.get(0).getTargetId());
			Assertions.assertEquals(received.get(0).getSeq(), source.health().getCursor());
		}
		finally {
			source.close();
		}
	}

	private PublishChainRequest chain(String chainId, String el, Long expectedVersion) {
		return PublishChainRequest.builder()
				.chainId(chainId)
				.el(el)
				.expectedVersion(expectedVersion)
				.build();
	}
}
