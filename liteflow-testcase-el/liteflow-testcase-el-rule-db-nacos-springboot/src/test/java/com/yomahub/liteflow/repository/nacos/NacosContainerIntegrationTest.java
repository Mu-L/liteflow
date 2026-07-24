package com.yomahub.liteflow.repository.nacos;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.config.ConfigService;
import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.PublishResult;
import com.yomahub.liteflow.publisher.RemoveRuleRequest;
import com.yomahub.liteflow.publisher.RulePublisher;
import com.yomahub.liteflow.publisher.RulePublisherFactory;
import com.yomahub.liteflow.publisher.exception.VersionConflictException;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Real Nacos CAS, listener, and repository coverage. */
@Testcontainers(disabledWithoutDocker = true)
class NacosContainerIntegrationTest {

	private static final int HTTP_PORT = 8848;
	private static final int GRPC_PORT = 9848;
	private static final String GRPC_OFFSET_PROPERTY = "nacos.server.grpc.port.offset";

	@Container
	private static final GenericContainer<?> NACOS = new GenericContainer<>(
			DockerImageName.parse("nacos/nacos-server:v2.5.3"))
			.withEnv("MODE", "standalone")
			.withEnv("NACOS_AUTH_ENABLE", "false")
			.withExposedPorts(HTTP_PORT, GRPC_PORT)
			.waitingFor(Wait.forHttp("/nacos/v1/console/health/readiness").forPort(HTTP_PORT))
			.withStartupTimeout(Duration.ofMinutes(2));

	private ConfigService client;
	private NacosConfigKey key;
	private NacosListenerChangeSource source;
	private String applicationName;
	private String previousGrpcOffset;

	@BeforeEach
	void setUp() throws Exception {
		previousGrpcOffset = System.getProperty(GRPC_OFFSET_PROPERTY);
		int offset = NACOS.getMappedPort(GRPC_PORT) - NACOS.getMappedPort(HTTP_PORT);
		System.setProperty(GRPC_OFFSET_PROPERTY, String.valueOf(offset));
		Properties properties = new Properties();
		properties.put(PropertyKeyConst.SERVER_ADDR,
				NACOS.getHost() + ":" + NACOS.getMappedPort(HTTP_PORT));
		client = NacosFactory.createConfigService(properties);
		applicationName = "it-" + UUID.randomUUID().toString().replace("-", "");
		key = new NacosConfigKey("lf", applicationName, "RULES");
	}

	@AfterEach
	void tearDown() throws Exception {
		try {
			if (source != null) { source.close(); }
			if (client != null) {
				if (key != null) { client.removeConfig(key.dataId(), key.group()); }
				client.shutDown();
			}
		}
		finally {
			if (previousGrpcOffset == null) {
				System.clearProperty(GRPC_OFFSET_PROPERTY);
			}
			else {
				System.setProperty(GRPC_OFFSET_PROPERTY, previousGrpcOffset);
			}
		}
	}

	@Test
	void publisherListenerAndRepositoryWorkAgainstRealNacos() throws Exception {
		NacosCatalogCodec codec = new NacosCatalogCodec();
		NacosConfigFacade facade = new ClientNacosConfigFacade(client);
		NacosCatalogStore store = new NacosCatalogStore(facade, key, 5000, codec);
		NacosRuleRepository repository = new NacosRuleRepository(store);
		List<ChangeRecord> changes = new CopyOnWriteArrayList<>();
		source = new NacosListenerChangeSource(store);
		source.open(changes::addAll);
		source.activate(repository.fetchManifest().getLatestSeq());

		NacosPublisherConfig config = NacosPublisherConfig.builder()
				.applicationName(applicationName).group(key.group()).dataIdPrefix("lf")
				.configService(client).timeoutMillis(5000L).build();
		try (RulePublisher publisher = RulePublisherFactory.create(config)) {
			PublishResult created = publisher.publishChain(PublishChainRequest.builder()
					.chainId("c1").el("THEN(a)").expectedVersion(0L).build());
			await(() -> changes.size() == 1);
			assertEquals(1, created.getVersion());
			assertEquals("THEN(a)", repository.fetchChain("c1").getEl());

			PublishResult updated = publisher.publishChain(PublishChainRequest.builder()
					.chainId("c1").el("THEN(a, b)").expectedVersion(1L).build());
			await(() -> changes.size() == 2);
			assertEquals(2, updated.getVersion());
			assertEquals("THEN(a, b)", repository.fetchChain("c1").getEl());
			assertThrows(VersionConflictException.class, () -> publisher.publishChain(
					PublishChainRequest.builder().chainId("c1").el("THEN(x)").expectedVersion(1L).build()));

			publisher.removeChain(RemoveRuleRequest.builder()
					.targetId("c1").expectedVersion(2L).build());
			await(() -> changes.size() == 3);
			assertNull(repository.fetchChain("c1"));
		}
		assertEquals(ChangeRecord.Op.UPSERT, changes.get(0).getOp());
		assertEquals(ChangeRecord.Op.DELETE, changes.get(2).getOp());
		assertEquals(3, source.health().getCursor());
	}

	private void await(BooleanSupplier condition) throws InterruptedException {
		long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
		while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
			Thread.sleep(25);
		}
		assertTrue(condition.getAsBoolean(), "timed out waiting for a Nacos listener event");
	}
}
