package com.yomahub.liteflow.test.ruledb.sql;

import com.yomahub.liteflow.core.FlowExecutor;
import com.yomahub.liteflow.flow.LiteflowResponse;
import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.PublishResult;
import com.yomahub.liteflow.publisher.PublishScriptRequest;
import com.yomahub.liteflow.publisher.RemoveRuleRequest;
import com.yomahub.liteflow.publisher.RulePublisher;
import com.yomahub.liteflow.publisher.RulePublisherFactory;
import com.yomahub.liteflow.publisher.exception.PublisherConfigurationException;
import com.yomahub.liteflow.publisher.exception.RuleValidationException;
import com.yomahub.liteflow.publisher.exception.VersionConflictException;
import com.yomahub.liteflow.repository.ChangeSourceHealth;
import com.yomahub.liteflow.repository.RuleChangeListener;
import com.yomahub.liteflow.repository.RuleDbSyncManager;
import com.yomahub.liteflow.repository.sql.SqlPollingChangeSource;
import com.yomahub.liteflow.repository.sql.SqlPublisherConfig;
import com.yomahub.liteflow.repository.sql.SqlRuleRepository;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import com.yomahub.liteflow.repository.vo.ScriptMeta;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@SpringBootTest(classes = RuleDbSqlApplication.class)
public class SqlPublisherContractTest {

	private static final String H2_URL = "jdbc:h2:mem:ruledb;DB_CLOSE_DELAY=-1;MODE=MySQL";

	private final SqlRuleRepository repository = new SqlRuleRepository();

	@Autowired
	private FlowExecutor flowExecutor;

	private RulePublisher publisher;

	@BeforeEach
	public void setUp() {
		publisher = RulePublisherFactory.create(SqlPublisherConfig.builder()
				.applicationName("ruledb-sql-it")
				.url(H2_URL)
				.username("sa")
				.password("")
				.build());
	}

	@AfterEach
	public void tearDown() {
		publisher.close();
	}

	@Test
	public void testCreateExactUpdateAndConflictRollback() {
		long before = repository.fetchLatestSeq();
		PublishResult created = publisher.publishChain(chain("casChain", "THEN(a)", 0L));
		Assertions.assertEquals(1L, created.getVersion());
		Assertions.assertEquals(before + 1, created.getSequence());

		PublishResult updated = publisher.publishChain(PublishChainRequest.builder()
				.chainId("casChain")
				.el("THEN(b)")
				.route("route-b")
				.namespace("ns-b")
				.expectedVersion(created.getVersion())
				.build());
		Assertions.assertEquals(2L, updated.getVersion());
		Assertions.assertEquals("route-b", repository.fetchChain("casChain").getRoute());
		Assertions.assertEquals("ns-b", repository.fetchChain("casChain").getNamespace());

		long beforeConflict = repository.fetchLatestSeq();
		Assertions.assertThrows(VersionConflictException.class,
				() -> publisher.publishChain(chain("casChain", "THEN(c)", 7L)));
		Assertions.assertEquals("THEN(b)", repository.fetchChain("casChain").getEl());
		Assertions.assertEquals(beforeConflict, repository.fetchLatestSeq());
	}

	@Test
	public void testScriptMetadataAndExactUpdate() {
		PublishResult created = publisher.publishScript(PublishScriptRequest.builder()
				.nodeId("casScript")
				.script("return true")
				.name("first")
				.type("boolean_script")
				.language("groovy")
				.expectedVersion(0L)
				.build());

		ScriptMeta first = repository.fetchScriptMeta("casScript");
		Assertions.assertEquals(created.getVersion(), first.getVersion());
		Assertions.assertEquals("boolean_script", first.getType());
		Assertions.assertEquals("groovy", first.getLanguage());
		Assertions.assertEquals("first", first.getName());

		publisher.publishScript(PublishScriptRequest.builder()
				.nodeId("casScript")
				.script("return 'a'")
				.name("second")
				.type("switch_script")
				.language("groovy")
				.expectedVersion(created.getVersion())
				.build());
		ScriptMeta second = repository.fetchScriptMeta("casScript");
		Assertions.assertEquals("switch_script", second.getType());
		Assertions.assertEquals("second", second.getName());
	}

	@Test
	public void testDeleteConflictDoesNotDeleteOrAppendChange() {
		PublishResult created = publisher.publishChain(chain("casDelete", "THEN(a)", 0L));
		long beforeConflict = repository.fetchLatestSeq();

		Assertions.assertThrows(VersionConflictException.class,
				() -> publisher.removeChain(RemoveRuleRequest.builder()
						.targetId("casDelete").expectedVersion(99L).build()));
		Assertions.assertNotNull(repository.fetchChain("casDelete"));
		Assertions.assertEquals(beforeConflict, repository.fetchLatestSeq());

		PublishResult removed = publisher.removeChain(RemoveRuleRequest.builder()
				.targetId("casDelete").expectedVersion(created.getVersion()).build());
		Assertions.assertEquals(ChangeRecord.Op.DELETE, removed.getOperation());
		Assertions.assertNull(repository.fetchChain("casDelete"));
		Assertions.assertEquals(beforeConflict + 1, removed.getSequence());
	}

	@Test
	public void testPollingSourceDeliversCompleteBatch() {
		long baseline = repository.fetchLatestSeq();
		SqlPollingChangeSource source = new SqlPollingChangeSource(repository, 3600);
		List<ChangeRecord> received = new ArrayList<>();
		source.open(new RuleChangeListener() {
			@Override
			public void onChanges(List<ChangeRecord> changes) {
				received.addAll(changes);
			}
		});
		source.activate(baseline);
		try {
			publisher.publishChain(chain("pollChain", "THEN(a)", 0L));
			source.pollOnce();

			Assertions.assertEquals(1, received.size());
			Assertions.assertEquals("pollChain", received.get(0).getTargetId());
			Assertions.assertEquals(received.get(0).getSeq(), source.health().getCursor());
		}
		finally {
			source.close();
		}
	}

	@Test
	public void testPollingSourceDrainsBoundedBatchesWithoutReconcile() {
		long baseline = repository.fetchLatestSeq();
		SqlPollingChangeSource source = new SqlPollingChangeSource(repository, 3600, 2);
		List<ChangeRecord> received = new ArrayList<>();
		List<Integer> batchSizes = new ArrayList<>();
		AtomicInteger reconciles = new AtomicInteger();
		source.open(new RuleChangeListener() {
			@Override
			public void onChanges(List<ChangeRecord> changes) {
				batchSizes.add(changes.size());
				received.addAll(changes);
			}

			@Override
			public void onReconcileRequired() {
				reconciles.incrementAndGet();
			}
		});
		source.activate(baseline);
		try {
			try (RulePublisher other = RulePublisherFactory.create(SqlPublisherConfig.builder()
					.applicationName("ruledb-sql-other-poll")
					.url(H2_URL)
					.username("sa")
					.password("")
					.build())) {
				other.publishChain(chain("otherPollChain", "THEN(a)", 0L));
			}
			for (int i = 0; i < 5; i++) {
				publisher.publishChain(chain("boundedPollChain" + i, "THEN(a)", 0L));
			}

			source.pollOnce();

			Assertions.assertFalse(source.requiresContinuousSequence());
			Assertions.assertEquals(5, received.size());
			Assertions.assertEquals(java.util.Arrays.asList(2, 2, 1), batchSizes);
			Assertions.assertEquals(0, reconciles.get());
			Assertions.assertEquals(repository.fetchLatestSeq(), source.health().getCursor());
		}
		finally {
			source.close();
		}
	}

	@Test
	public void testManualContentAndVersionUpdateConvergesWithoutChangeLog() throws Exception {
		publisher.publishChain(chain("manualChain", "THEN(a, b)", 0L));
		RuleDbSyncManager.reconcileOnce();
		Assertions.assertEquals("a==>b", execute("manualChain").getExecuteStepStr());

		long sequence = repository.fetchLatestSeq();
		String sql = "UPDATE lf_chain SET el_data = ?, version = version + 1 "
				+ "WHERE application_name = ? AND chain_id = ?";
		try (Connection connection = DriverManager.getConnection(H2_URL, "sa", "");
				PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setString(1, "THEN(b, a)");
			statement.setString(2, "ruledb-sql-it");
			statement.setString(3, "manualChain");
			Assertions.assertEquals(1, statement.executeUpdate());
		}

		Assertions.assertEquals(sequence, repository.fetchLatestSeq());
		RuleDbSyncManager.reconcileOnce();
		Assertions.assertEquals("b==>a", execute("manualChain").getExecuteStepStr());
	}

	@Test
	public void testInvalidConfigurationAndSchemaBoundariesAreRejectedBeforeJdbc() {
		Assertions.assertThrows(PublisherConfigurationException.class,
				() -> RulePublisherFactory.create(SqlPublisherConfig.builder()
						.applicationName("app").url(H2_URL).tablePrefix("lf_;drop_").build()));
		Assertions.assertThrows(PublisherConfigurationException.class,
				() -> RulePublisherFactory.create(SqlPublisherConfig.builder()
						.applicationName("app").url(H2_URL).tablePrefix(repeat("p", 55)).build()));
		Assertions.assertThrows(PublisherConfigurationException.class,
				() -> RulePublisherFactory.create(SqlPublisherConfig.builder()
						.applicationName(repeat("a", 65)).url(H2_URL).build()));

		Assertions.assertThrows(RuleValidationException.class,
				() -> publisher.publishChain(chain(repeat("c", 129), "THEN(a)", null)));
		Assertions.assertThrows(RuleValidationException.class,
				() -> publisher.publishChain(PublishChainRequest.builder()
						.chainId("longNamespace").el("THEN(a)").namespace(repeat("n", 65)).build()));
		Assertions.assertThrows(RuleValidationException.class,
				() -> publisher.publishChain(chain("largeEl", repeat("a", 65536), null)));
		Assertions.assertThrows(RuleValidationException.class,
				() -> publisher.publishScript(PublishScriptRequest.builder()
						.nodeId("longLanguage").script("return true").type("script")
						.language(repeat("l", 33)).build()));
		Assertions.assertThrows(RuleValidationException.class,
				() -> publisher.removeChain(RemoveRuleRequest.builder().targetId(repeat("r", 129)).build()));

		try (RulePublisher boundaryPublisher = RulePublisherFactory.create(SqlPublisherConfig.builder()
				.applicationName(repeat("a", 64)).url(H2_URL).username("sa").password("").build())) {
			boundaryPublisher.publishChain(PublishChainRequest.builder()
					.chainId(repeat("c", 128)).el(repeat("e", 65535))
					.namespace(repeat("n", 64)).build());
			boundaryPublisher.publishScript(PublishScriptRequest.builder()
					.nodeId(repeat("s", 128)).script(repeat("x", 65535)).name(repeat("m", 128))
					.type("script").language(repeat("l", 32)).build());
		}
	}

	@Test
	public void testCorruptChangeLogRequestsReconcileWithoutAdvancingCursor() throws Exception {
		long baseline = repository.fetchLatestSeq();
		try (Connection connection = DriverManager.getConnection(H2_URL, "sa", "");
				PreparedStatement statement = connection.prepareStatement(
						"INSERT INTO lf_change_log (application_name, target_type, target_id, op, version) "
								+ "VALUES (?, ?, ?, ?, ?)")) {
			statement.setString(1, "ruledb-sql-it");
			statement.setString(2, "CORRUPT");
			statement.setString(3, "poisonRow");
			statement.setString(4, "UPSERT");
			statement.setLong(5, 1);
			statement.executeUpdate();
		}

		AtomicInteger reconciles = new AtomicInteger();
		SqlPollingChangeSource source = new SqlPollingChangeSource(repository, 3600, 10);
		source.open(new RuleChangeListener() {
			@Override
			public void onChanges(List<ChangeRecord> changes) {
				Assertions.fail("corrupt batches must not be partially delivered");
			}

			@Override
			public void onReconcileRequired() {
				reconciles.incrementAndGet();
			}
		});
		source.activate(baseline);
		try {
			source.pollOnce();
			Assertions.assertEquals(1, reconciles.get());
			Assertions.assertEquals(baseline, source.health().getCursor());
			Assertions.assertEquals(ChangeSourceHealth.Status.DEGRADED, source.health().getStatus());
		}
		finally {
			source.close();
			try (Connection connection = DriverManager.getConnection(H2_URL, "sa", "");
					PreparedStatement statement = connection.prepareStatement(
							"DELETE FROM lf_change_log WHERE application_name = ? AND target_id = ?")) {
				statement.setString(1, "ruledb-sql-it");
				statement.setString(2, "poisonRow");
				statement.executeUpdate();
			}
		}
	}

	private LiteflowResponse execute(String chainId) {
		LiteflowResponse response = flowExecutor.execute2Resp(chainId, "arg");
		Assertions.assertTrue(response.isSuccess(), response.getCause() == null
				? "chain execution failed" : response.getCause().getMessage());
		return response;
	}

	private PublishChainRequest chain(String chainId, String el, Long expectedVersion) {
		return PublishChainRequest.builder()
				.chainId(chainId)
				.el(el)
				.expectedVersion(expectedVersion)
				.build();
	}

	private String repeat(String value, int count) {
		StringBuilder result = new StringBuilder(value.length() * count);
		for (int i = 0; i < count; i++) {
			result.append(value);
		}
		return result.toString();
	}
}
