/**
 * <p>Title: liteflow</p>
 * <p>Description: 轻量级的组件式流程框架</p>
 */
package com.yomahub.liteflow.test.ruledb.sql.config;

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
 * {@code flowExecutor.init(true)} 之前注入，Rule-DB 模式得以激活。
 *
 * <p>H2 容器 DataSource 由 {@code spring.datasource.*} + DataSourceAutoConfiguration 产生，
 * rule-db.url 留空，{@code SqlConnectionManager} 自动按类型查找该 DataSource。
 *
 * @author Bryan.Zhang
 * @since 2.16.2
 */
@Configuration
public class RuleDbTestConfig {

	@Autowired
	private LiteflowConfig liteflowConfig;

	@PostConstruct
	public void wireRuleDb() {
		RuleDbConfig ruleDb = new RuleDbConfig();
		ruleDb.setEnabled(true);
		ruleDb.setApplicationName("ruledb-sql-it");
		// H2 启动期自动建 lf_chain / lf_script / lf_change_log 三张表
		ruleDb.setAutoInitTable(true);
		ruleDb.setSeqPollSeconds(1);
		ruleDb.setReconcileSeconds(60);
		liteflowConfig.setRuleDb(ruleDb);
	}
}
