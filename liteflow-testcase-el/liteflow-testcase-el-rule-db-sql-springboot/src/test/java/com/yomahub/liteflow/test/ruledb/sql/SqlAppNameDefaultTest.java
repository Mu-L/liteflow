/**
 * <p>Title: liteflow</p>
 * <p>Description: 轻量级的组件式流程框架</p>
 */
package com.yomahub.liteflow.test.ruledb.sql;

import com.yomahub.liteflow.property.LiteflowConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * 配置推断回归：`liteflow.rule-db.application-name` 未显式配置时，
 * starter 必须默认取 `spring.application.name`（本模块 properties 只配了后者）。
 * 这是多应用共库的隔离维度——不默认会全部落进 "default" 命名空间互相污染。
 *
 * @author Bryan.Zhang
 * @since 2.16.1
 */
@SpringBootTest(classes = RuleDbSqlApplication.class)
public class SqlAppNameDefaultTest {

	@Autowired
	private LiteflowConfig liteflowConfig;

	@Test
	public void testApplicationNameDefaultsToSpringApplicationName() {
		Assertions.assertNotNull(liteflowConfig.getRuleDb());
		Assertions.assertEquals("ruledb-sql-it", liteflowConfig.getRuleDb().getApplicationName(),
				"application-name should default to spring.application.name when not set");
	}

}
