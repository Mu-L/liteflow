/**
 * <p>Title: liteflow</p>
 * <p>Description: 轻量级的组件式流程框架</p>
 */
package com.yomahub.liteflow.test.ruledb.redis;

import cn.hutool.crypto.SecureUtil;
import com.yomahub.liteflow.core.FlowExecutor;
import com.yomahub.liteflow.exception.SeqGapException;
import com.yomahub.liteflow.flow.LiteflowResponse;
import com.yomahub.liteflow.repository.RuleDbSyncManager;
import com.yomahub.liteflow.repository.redis.RedisRulePublisher;
import com.yomahub.liteflow.repository.redis.RedisRuleRepository;
import com.yomahub.liteflow.repository.vo.ChainMeta;
import com.yomahub.liteflow.repository.vo.ChainRecord;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import com.yomahub.liteflow.repository.vo.RuleManifest;
import com.yomahub.liteflow.repository.vo.ScriptRecord;
import com.yomahub.liteflow.slot.DefaultContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import redis.embedded.RedisServer;
import redis.embedded.core.ExecutableProvider;

import java.util.List;

/**
 * Rule-DB Redis 插件端到端集成测试：embedded-redis 拉起真实 redis 二进制，
 * 验证 Lua 原子发布（HSET 内容 + HSET index + INCR seq + ZADD changelog + PUBLISH）→
 * FlowExecutor 惰性回源（RedisRuleRepository）→ 执行 → 发布新版本 → pollOnce 收敛到新逻辑。
 *
 * <p>类名以 {@code Test} 结尾以匹配 maven-surefire-plugin 的默认 includes
 * （本仓库未配置 failsafe，{@code *IT} 不会被 surefire 收集）。
 *
 * <p>确定性收敛：直接调用 {@link RuleDbSyncManager#reconcileOnce()} /
 * {@link RuleDbSyncManager#pollOnce()} 驱动同步路径，不依赖 sleep 或定时器调度。
 *
 * @author Bryan.Zhang
 * @since 2.16.1
 */
@SpringBootTest(classes = RuleDbRedisApplication.class)
public class RuleDbRedisTest {

	private static RedisServer redisServer;

	@Autowired
	private FlowExecutor flowExecutor;

	@BeforeAll
	public static void startRedis() throws Exception {
		// 非默认端口 16379，避免与开发者本地 redis 冲突。
		// 先用 embedded-redis 自带二进制（Linux CI 通常直接可用）；若自带二进制无法运行
		// （如 macOS ARM 缺 openssl），回退到 PATH 上的系统 redis-server——两者都是真实 redis
		// 二进制，跑相同的 Lua/cjson + pub/sub 路径，不削弱任何断言。
		redisServer = RedisServer.newRedisServer().port(16379).build();
		try {
			redisServer.start();
		} catch (Exception e) {
			try {
				redisServer.stop();
			} catch (Exception ignore) {
				// 忽略部分启动后的停止异常
			}
			redisServer = RedisServer.newRedisServer()
					.executableProvider(ExecutableProvider.newExecutableInPath("redis-server"))
					.port(16379)
					.build();
			redisServer.start();
		}
		// FLUSHALL：redis 数据可能跨测试运行残留（RDB 落盘重载/残留进程），
		// 协议测试断言"新 id 首发版本=1"，必须保证从空库开始
		Config config = new Config();
		config.useSingleServer().setAddress("redis://127.0.0.1:16379");
		RedissonClient flushClient = Redisson.create(config);
		try {
			flushClient.getKeys().flushall();
		} finally {
			flushClient.shutdown();
		}
	}

	@AfterAll
	public static void stopRedis() throws Exception {
		if (redisServer != null) {
			redisServer.stop();
		}
	}

	@Test
	public void testPublishExecuteAndConverge() {
		RedisRulePublisher publisher = new RedisRulePublisher();
		publisher.publishChain("rchainA", "THEN(a, b)");
		// 全量对账把新发布的 chain 纳入索引（显式对账更稳，不依赖启动期 manifest）
		RuleDbSyncManager.reconcileOnce();

		LiteflowResponse r1 = flowExecutor.execute2Resp("rchainA", "arg");
		Assertions.assertTrue(r1.isSuccess());
		Assertions.assertEquals("a==>b", r1.getExecuteStepStr());

		// 发布新版本（执行顺序反转），通过 seq 轮询确定性收敛
		publisher.publishChain("rchainA", "THEN(b, a)");
		RuleDbSyncManager.pollOnce();

		LiteflowResponse r2 = flowExecutor.execute2Resp("rchainA", "arg");
		Assertions.assertTrue(r2.isSuccess());
		Assertions.assertEquals("b==>a", r2.getExecuteStepStr());
	}

	@Test
	public void testRedisDefaultSeqPollSecondsIsRelaxed() {
		// Redis 有 pub/sub 推送，seq 轮询只是兜底：插件级默认 30s（SQL 无推送，默认 3s）
		Assertions.assertEquals(30, new RedisRuleRepository().defaultSeqPollSeconds());
	}

	@Test
	public void testLuaPublishProtocolStructures() {
		RedisRulePublisher publisher = new RedisRulePublisher();
		RedisRuleRepository repository = new RedisRuleRepository();
		long before = repository.fetchLatestSeq();

		long v1 = publisher.publishChain("protoR", "THEN(a, b)");
		Assertions.assertEquals(1, v1, "first publish must return version 1");

		// 内容 HASH：el/version/md5/enable
		ChainRecord r = repository.fetchChain("protoR");
		Assertions.assertEquals("THEN(a, b)", r.getEl());
		Assertions.assertEquals(1, r.getVersion());
		Assertions.assertEquals(SecureUtil.md5("THEN(a, b)"), r.getMd5());
		Assertions.assertTrue(r.isEnable());
		ChainMeta meta = repository.fetchChainMeta("protoR");
		Assertions.assertNotNull(meta);
		Assertions.assertEquals(1, meta.getVersion());
		Assertions.assertEquals(r.getMd5(), meta.getMd5());

		// seq 自增 + changelog ZSet 落一条同构 JSON
		Assertions.assertEquals(before + 1, repository.fetchLatestSeq());
		List<ChangeRecord> changes = repository.fetchChangesSince(before);
		Assertions.assertEquals(1, changes.size());
		Assertions.assertEquals(ChangeRecord.Op.UPSERT, changes.get(0).getOp());
		Assertions.assertEquals(ChangeRecord.TargetType.CHAIN, changes.get(0).getTargetType());
		Assertions.assertEquals("protoR", changes.get(0).getTargetId());
		Assertions.assertEquals(before + 1, changes.get(0).getSeq());
		Assertions.assertEquals(1, changes.get(0).getVersion());

		// index HASH 供 manifest 一次 HGETALL
		RuleManifest m = repository.fetchManifest();
		Assertions.assertTrue(m.getChains().stream().anyMatch(cm ->
				cm.getChainId().equals("protoR")
						&& cm.getVersion() == 1
						&& SecureUtil.md5("THEN(a, b)").equals(cm.getMd5())));
	}

	@Test
	public void testVersionIncrementsAcrossPublishes() {
		RedisRulePublisher publisher = new RedisRulePublisher();
		Assertions.assertEquals(1, publisher.publishChain("verChain", "THEN(a, b)"));
		Assertions.assertEquals(2, publisher.publishChain("verChain", "THEN(b, a)"));
		Assertions.assertEquals(3, publisher.publishChain("verChain", "THEN(a, b)"));
		Assertions.assertEquals(3, new RedisRuleRepository().fetchChain("verChain").getVersion());
	}

	@Test
	public void testPublishWithRouteAndNamespace() {
		RedisRulePublisher publisher = new RedisRulePublisher();
		publisher.publishChain("routedChain", "THEN(a, b)", "AND(a)", "ns-redis");

		ChainRecord r = new RedisRuleRepository().fetchChain("routedChain");
		Assertions.assertEquals("THEN(a, b)", r.getEl());
		Assertions.assertEquals("AND(a)", r.getRoute());
		Assertions.assertEquals("ns-redis", r.getNamespace());
	}

	@Test
	public void testRemoveChainConvergesViaPoll() {
		RedisRulePublisher publisher = new RedisRulePublisher();
		RedisRuleRepository repository = new RedisRuleRepository();
		publisher.publishChain("rDel", "THEN(a, b)");
		RuleDbSyncManager.reconcileOnce();
		Assertions.assertTrue(flowExecutor.execute2Resp("rDel", "arg").isSuccess());

		publisher.removeChain("rDel");
		// pub/sub 推送通常已先行应用，pollOnce 确保确定性收敛（两条路径幂等）
		RuleDbSyncManager.pollOnce();

		Assertions.assertNull(repository.fetchChain("rDel"));
		Assertions.assertFalse(flowExecutor.execute2Resp("rDel", "arg").isSuccess());
	}

	@Test
	public void testChangelogTrimTriggersSeqGap() {
		RedisRulePublisher publisher = new RedisRulePublisher();
		RedisRuleRepository repository = new RedisRuleRepository();

		// 保证 since > 0（断档检测只在非冷启动位点生效）
		publisher.publishChain("gapChain", "THEN(a, b)");
		long s0 = repository.fetchLatestSeq();
		publisher.publishChain("gapChain", "THEN(a, b)");
		long s1 = repository.fetchLatestSeq();
		publisher.publishChain("gapChain", "THEN(a, b)");

		// 运维裁剪 changelog：删除 score <= s1 的成员，制造断档
		Config config = new Config();
		config.useSingleServer().setAddress("redis://127.0.0.1:16379");
		RedissonClient trimClient = Redisson.create(config);
		try {
			RScoredSortedSet<String> log = trimClient.getScoredSortedSet(
					"lf:ruledb-redis-it:changelog", StringCodec.INSTANCE);
			log.removeRangeByScore(0, true, s1, true);
		} finally {
			trimClient.shutdown();
		}

		Assertions.assertThrows(SeqGapException.class, () -> repository.fetchChangesSince(s0));
	}

	@Test
	public void testBackgroundConvergenceWithoutManualSync() throws Exception {
		RedisRulePublisher publisher = new RedisRulePublisher();
		publisher.publishChain("pushChain", "THEN(a, b)");
		RuleDbSyncManager.reconcileOnce();
		Assertions.assertEquals("a==>b", flowExecutor.execute2Resp("pushChain", "arg").getExecuteStepStr());

		// 只发布、不手动驱动同步：靠 notify 频道推送（毫秒级）/ 后台 seq 轮询（1s）收敛
		publisher.publishChain("pushChain", "THEN(b, a)");
		long deadline = System.currentTimeMillis() + 5000;
		String step = "";
		while (System.currentTimeMillis() < deadline) {
			step = flowExecutor.execute2Resp("pushChain", "arg").getExecuteStepStr();
			if ("b==>a".equals(step)) {
				break;
			}
			Thread.sleep(50);
		}
		Assertions.assertEquals("b==>a", step, "publish should converge via push/background polling without manual sync");
	}

	@Test
	public void testScriptPublishAndExecute() {
		RedisRulePublisher publisher = new RedisRulePublisher();
		ScriptRecord s = new ScriptRecord();
		s.setNodeId("rS1");
		s.setType("script");
		s.setLanguage("groovy");
		s.setScript("defaultContext.setData(\"rS1\", true);");
		publisher.publishScript(s);
		publisher.publishChain("rchainS", "THEN(a, rS1)");
		RuleDbSyncManager.reconcileOnce();

		LiteflowResponse r = flowExecutor.execute2Resp("rchainS", "arg");
		Assertions.assertTrue(r.isSuccess());
		Assertions.assertEquals(Boolean.TRUE,
				r.getContextBean(DefaultContext.class).getData("rS1"));
	}

}
