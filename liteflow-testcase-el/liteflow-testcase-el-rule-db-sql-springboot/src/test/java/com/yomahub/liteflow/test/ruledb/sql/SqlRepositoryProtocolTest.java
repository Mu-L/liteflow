/**
 * <p>Title: liteflow</p>
 * <p>Description: 轻量级的组件式流程框架</p>
 */
package com.yomahub.liteflow.test.ruledb.sql;

import cn.hutool.crypto.SecureUtil;
import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.PublishScriptRequest;
import com.yomahub.liteflow.publisher.RemoveRuleRequest;
import com.yomahub.liteflow.publisher.RulePublisher;
import com.yomahub.liteflow.publisher.RulePublisherFactory;
import com.yomahub.liteflow.repository.sql.SqlPublisherConfig;
import com.yomahub.liteflow.repository.sql.SqlRulePublisher;
import com.yomahub.liteflow.repository.sql.SqlRuleRepository;
import com.yomahub.liteflow.repository.vo.ChainMeta;
import com.yomahub.liteflow.repository.vo.ChainRecord;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import com.yomahub.liteflow.repository.vo.RuleManifest;
import com.yomahub.liteflow.repository.vo.ScriptRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * SQL 发布/读取协议回归（真实 H2）：版本单调自增、change_log 逐条落库、
 * DELETE 行为、manifest 内容、断档检测、并发发布的版本原子性。
 *
 * <p>共享同一 H2 内存库与 application_name，因此所有断言都使用本类独有的
 * chain/script id，且以相对 seq（先取 fetchLatestSeq 再比较增量）方式书写，
 * 与其他测试类的发布互不干扰。
 *
 * @author Bryan.Zhang
 * @since 2.16.1
 */
@SpringBootTest(classes = RuleDbSqlApplication.class)
public class SqlRepositoryProtocolTest {

	private static final String H2_URL = "jdbc:h2:mem:ruledb;DB_CLOSE_DELAY=-1;MODE=MySQL";

	private final RulePublisher publisher = RulePublisherFactory.create(SqlPublisherConfig.builder()
			.applicationName("ruledb-sql-it")
			.url(H2_URL)
			.username("sa")
			.password("")
			.build());

	private final SqlRuleRepository repository = new SqlRuleRepository();

	@Test
	public void testPublishVersionAndChangeLogProtocol() {
		long before = repository.fetchLatestSeq();

		long v1 = publishChain("protoChain", "THEN(a, b)");
		long v2 = publishChain("protoChain", "THEN(b, a)");
		Assertions.assertEquals(v1 + 1, v2, "republish must bump version by exactly 1");

		// 内容行以最后一次发布为准，md5 重算
		ChainRecord r = repository.fetchChain("protoChain");
		Assertions.assertEquals("THEN(b, a)", r.getEl());
		Assertions.assertEquals(v2, r.getVersion());
		Assertions.assertEquals(SecureUtil.md5("THEN(b, a)"), r.getMd5());
		Assertions.assertTrue(r.isEnable());
		ChainMeta meta = repository.fetchChainMeta("protoChain");
		Assertions.assertNotNull(meta);
		Assertions.assertEquals(v2, meta.getVersion());
		Assertions.assertEquals(r.getMd5(), meta.getMd5());

		// change_log 两条 UPSERT，seq 严格递增，版本对应
		List<ChangeRecord> changes = repository.fetchChangesSince(before);
		Assertions.assertEquals(2, changes.size());
		Assertions.assertEquals(ChangeRecord.Op.UPSERT, changes.get(0).getOp());
		Assertions.assertEquals("protoChain", changes.get(0).getTargetId());
		Assertions.assertEquals(v1, changes.get(0).getVersion());
		Assertions.assertEquals(v2, changes.get(1).getVersion());
		Assertions.assertTrue(changes.get(0).getSeq() < changes.get(1).getSeq());
		Assertions.assertEquals(before + 2, repository.fetchLatestSeq());
	}

	@Test
	public void testRemoveChainClearsRowAndLogsDelete() {
		long version = publishChain("protoDelChain", "THEN(a, b)");
		Assertions.assertNotNull(repository.fetchChain("protoDelChain"));

		long before = repository.fetchLatestSeq();
		publisher.removeChain(RemoveRuleRequest.builder().targetId("protoDelChain").build());

		Assertions.assertNull(repository.fetchChain("protoDelChain"));
		List<ChangeRecord> changes = repository.fetchChangesSince(before);
		Assertions.assertEquals(1, changes.size());
		Assertions.assertEquals(ChangeRecord.Op.DELETE, changes.get(0).getOp());
		Assertions.assertEquals(ChangeRecord.TargetType.CHAIN, changes.get(0).getTargetType());
		Assertions.assertEquals("protoDelChain", changes.get(0).getTargetId());
		Assertions.assertEquals(version, changes.get(0).getVersion());
	}

	@Test
	public void testRemoveScriptClearsRowAndLogsDelete() {
		ScriptRecord s = new ScriptRecord();
		s.setNodeId("protoDelScript");
		s.setType("script");
		s.setLanguage("groovy");
		s.setScript("println('bye')");
		long version = publishScript(s);
		Assertions.assertNotNull(repository.fetchScript("protoDelScript"));

		long before = repository.fetchLatestSeq();
		publisher.removeScript(RemoveRuleRequest.builder().targetId("protoDelScript").build());

		Assertions.assertNull(repository.fetchScript("protoDelScript"));
		List<ChangeRecord> changes = repository.fetchChangesSince(before);
		Assertions.assertEquals(1, changes.size());
		Assertions.assertEquals(ChangeRecord.Op.DELETE, changes.get(0).getOp());
		Assertions.assertEquals(ChangeRecord.TargetType.SCRIPT, changes.get(0).getTargetType());
		Assertions.assertEquals(version, changes.get(0).getVersion());
	}

	@Test
	public void testManifestListsPublishedEntries() {
		publishChain("protoManifestChain", "THEN(a, b)");
		ScriptRecord s = new ScriptRecord();
		s.setNodeId("protoManifestScript");
		s.setType("boolean_script");
		s.setLanguage("groovy");
		s.setScript("return true");
		publishScript(s);

		RuleManifest m = repository.fetchManifest();
		Assertions.assertTrue(m.getChains().stream().anyMatch(cm ->
				cm.getChainId().equals("protoManifestChain")
						&& cm.getVersion() >= 1
						&& SecureUtil.md5("THEN(a, b)").equals(cm.getMd5())));
		Assertions.assertTrue(m.getScripts().stream().anyMatch(sm ->
				sm.getNodeId().equals("protoManifestScript")
						&& "boolean_script".equals(sm.getType())
						&& "groovy".equals(sm.getLanguage())
						&& SecureUtil.md5("return true").equals(sm.getMd5())));
		Assertions.assertTrue(m.getLatestSeq() >= 2);
	}

	@Test
	public void testApplicationScopedSequenceAllowsGlobalGaps() {
		long before = repository.fetchLatestSeq();
		try (RulePublisher other = RulePublisherFactory.create(SqlPublisherConfig.builder()
				.applicationName("ruledb-sql-other")
				.url(H2_URL)
				.username("sa")
				.password("")
				.build())) {
			other.publishChain(PublishChainRequest.builder()
					.chainId("otherAppChain").el("THEN(a)").build());
		}
		publishChain("afterGlobalGap", "THEN(b)");

		List<ChangeRecord> changes = repository.fetchChangesSince(before);
		Assertions.assertEquals(1, changes.size());
		Assertions.assertEquals("afterGlobalGap", changes.get(0).getTargetId());
	}

	@Test
	public void testChangeLogReadsAreBoundedByBatchSize() {
		long before = repository.fetchLatestSeq();
		for (int i = 0; i < 5; i++) {
			publishChain("batchChain" + i, "THEN(a)");
		}

		List<ChangeRecord> first = repository.fetchChangesSince(before, 2);
		Assertions.assertEquals(2, first.size());
		List<ChangeRecord> second = repository.fetchChangesSince(first.get(1).getSeq(), 2);
		Assertions.assertEquals(2, second.size());
		List<ChangeRecord> third = repository.fetchChangesSince(second.get(1).getSeq(), 2);
		Assertions.assertEquals(1, third.size());
	}

	@Test
	public void testConcurrentRepublishKeepsVersionAtomic() throws Exception {
		// 首发单独完成；并发重发布走行锁自增。
		publishChain("protoCcChain", "THEN(a, b)");
		long before = repository.fetchLatestSeq();

		int threads = 2, publishesPerThread = 5;
		ExecutorService pool = Executors.newFixedThreadPool(threads);
		CountDownLatch start = new CountDownLatch(1);
		AtomicInteger failures = new AtomicInteger();
		List<Runnable> tasks = new ArrayList<>();
		for (int t = 0; t < threads; t++) {
			tasks.add(() -> {
				try {
					start.await();
					for (int i = 0; i < publishesPerThread; i++) {
						publishChain("protoCcChain", "THEN(b, a)");
					}
				} catch (Exception e) {
					failures.incrementAndGet();
				}
			});
		}
		tasks.forEach(pool::submit);
		start.countDown();
		pool.shutdown();
		Assertions.assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
		Assertions.assertEquals(0, failures.get(), "no publish should fail under row-lock increment");

		// 10 次并发重发布不丢更新：版本恰好 +10，change_log 恰好多 10 条
		Assertions.assertEquals(1 + threads * publishesPerThread,
				repository.fetchChain("protoCcChain").getVersion());
		Assertions.assertEquals(threads * publishesPerThread,
				repository.fetchChangesSince(before).size());
	}

	@Test
	public void testConcurrentFirstPublishRetriesUniqueKeyRace() throws Exception {
		String chainId = "protoConcurrentCreate" + System.nanoTime();
		long before = repository.fetchLatestSeq();
		int threads = 4;
		ExecutorService pool = Executors.newFixedThreadPool(threads);
		CountDownLatch start = new CountDownLatch(1);
		List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
		for (int i = 0; i < threads; i++) {
			final int component = i;
			pool.submit(() -> {
				try {
					start.await();
					publishChain(chainId, component % 2 == 0 ? "THEN(a)" : "THEN(b)");
				}
				catch (Throwable e) {
					failures.add(e);
				}
			});
		}
		start.countDown();
		pool.shutdown();
		Assertions.assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
		Assertions.assertTrue(failures.isEmpty(), "all concurrent creates must succeed: " + failures);
		Assertions.assertEquals(threads, repository.fetchChain(chainId).getVersion());
		Assertions.assertEquals(threads, repository.fetchChangesSince(before).size());
	}

	@Test
	public void testLegacyPublisherConcurrentFirstPublishRetriesUniqueKeyRace() throws Exception {
		String chainId = "legacyConcurrentCreate" + System.nanoTime();
		long before = repository.fetchLatestSeq();
		int threads = 4;
		SqlRulePublisher legacyPublisher = new SqlRulePublisher();
		ExecutorService pool = Executors.newFixedThreadPool(threads);
		CountDownLatch start = new CountDownLatch(1);
		List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
		for (int i = 0; i < threads; i++) {
			final int component = i;
			pool.submit(() -> {
				try {
					start.await();
					legacyPublisher.publishChain(chainId, component % 2 == 0 ? "THEN(a)" : "THEN(b)");
				}
				catch (Throwable e) {
					failures.add(e);
				}
			});
		}
		start.countDown();
		pool.shutdown();
		Assertions.assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
		Assertions.assertTrue(failures.isEmpty(), "legacy concurrent creates must succeed: " + failures);
		Assertions.assertEquals(threads, repository.fetchChain(chainId).getVersion());
		Assertions.assertEquals(threads, repository.fetchChangesSince(before).size());
	}

	private long publishChain(String chainId, String el) {
		return publisher.publishChain(PublishChainRequest.builder()
				.chainId(chainId).el(el).build()).getVersion();
	}

	private long publishScript(ScriptRecord script) {
		return publisher.publishScript(PublishScriptRequest.builder()
				.nodeId(script.getNodeId())
				.script(script.getScript())
				.name(script.getName())
				.type(script.getType())
				.language(script.getLanguage())
				.build()).getVersion();
	}

}
