/**
 * <p>Title: liteflow</p>
 * <p>Description: 轻量级的组件式流程框架</p>
 */
package com.yomahub.liteflow.test.ruledb.redis;

import com.yomahub.liteflow.core.FlowExecutor;
import com.yomahub.liteflow.flow.LiteflowResponse;
import com.yomahub.liteflow.repository.RuleDbSyncManager;
import com.yomahub.liteflow.repository.redis.RedisRulePublisher;
import com.yomahub.liteflow.repository.vo.ScriptRecord;
import com.yomahub.liteflow.slot.DefaultContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import redis.embedded.RedisServer;
import redis.embedded.core.ExecutableProvider;

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
 * @since 2.16.2
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
