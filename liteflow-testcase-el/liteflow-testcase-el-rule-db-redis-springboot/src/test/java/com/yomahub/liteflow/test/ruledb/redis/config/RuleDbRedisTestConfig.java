/**
 * <p>Title: liteflow</p>
 * <p>Description: 轻量级的组件式流程框架</p>
 */
package com.yomahub.liteflow.test.ruledb.redis.config;

import com.yomahub.liteflow.property.LiteflowConfig;
import com.yomahub.liteflow.property.RuleDbConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;

/**
 * 计划 3（starter 的 liteflow.rule-db.* 绑定）尚未完成，故本测试以编程方式装配
 * {@link RuleDbConfig}：在单例 bean 初始化阶段（{@code @PostConstruct}）把 ruleDb 设进
 * starter 创建的 {@link LiteflowConfig} 单例。
 *
 * <p>时序保证：{@code @PostConstruct} 在所有单例 bean 实例化阶段执行，而
 * {@code LiteflowExecutorInit}（{@link org.springframework.beans.factory.SmartInitializingSingleton}）
 * 的 {@code afterSingletonsInstantiated} 在所有单例实例化完成后才回调，因此 ruleDb 一定在
 * {@code flowExecutor.init(true)} 之前注入，Rule-DB 模式得以激活；后续 RedisRulePublisher /
 * RedisRuleRepository 读取 {@link com.yomahub.liteflow.property.LiteflowConfigGetter} 时也一致。
 *
 * <p>address 指向 embedded-redis 监听的 16379（非默认端口，避免与开发者本地 redis 冲突）。
 *
 * @author Bryan.Zhang
 * @since 2.16.1
 */
@Configuration
public class RuleDbRedisTestConfig {

	@Autowired
	private LiteflowConfig liteflowConfig;

	@PostConstruct
	public void wireRuleDb() {
		RuleDbConfig ruleDb = new RuleDbConfig();
		ruleDb.setEnabled(true);
		ruleDb.setApplicationName("ruledb-redis-it");
		ruleDb.setAddress("redis://127.0.0.1:16379");
		ruleDb.setSeqPollSeconds(1);
		ruleDb.setReconcileSeconds(60);
		liteflowConfig.setRuleDb(ruleDb);
	}
}
