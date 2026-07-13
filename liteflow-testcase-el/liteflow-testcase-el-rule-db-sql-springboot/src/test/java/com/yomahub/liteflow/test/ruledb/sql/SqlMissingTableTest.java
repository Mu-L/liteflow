/**
 * <p>Title: liteflow</p>
 * <p>Description: 轻量级的组件式流程框架</p>
 */
package com.yomahub.liteflow.test.ruledb.sql;

import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.property.LiteflowConfig;
import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.repository.sql.SqlRuleRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * 缺表且未开 auto-init-table：报错必须是 ConfigErrorException 且信息内含完整可复制执行的 DDL（spec §9），
 * 而不是让后续查询抛一个裸 SQLException 让用户自己猜。
 *
 * <p>纯 JUnit 测试，不拉 Spring 上下文：直接把配置放进 LiteflowConfigGetter，
 * SqlRuleRepository 指向一个没有任何表的全新 H2 内存库。
 *
 * @author Bryan.Zhang
 * @since 2.16.1
 */
public class SqlMissingTableTest {

	@AfterEach
	public void cleanup() {
		LiteflowConfigGetter.clean();
	}

	@Test
	public void testMissingTablesErrorContainsDdl() {
		LiteflowConfig config = new LiteflowConfig();
		RuleDbConfig ruleDb = new RuleDbConfig();
		// 全新内存库，无表；不开 auto-init-table
		ruleDb.getSql().setUrl("jdbc:h2:mem:lfNoTables;DB_CLOSE_DELAY=-1");
		ruleDb.getSql().setAutoInitTable(false);
		config.setRuleDb(ruleDb);
		LiteflowConfigGetter.setLiteflowConfig(config);

		ConfigErrorException e = Assertions.assertThrows(ConfigErrorException.class,
				() -> new SqlRuleRepository().fetchManifest());
		Assertions.assertTrue(e.getMessage().contains("CREATE TABLE"),
				"error message should contain copy-pastable DDL, but was: " + e.getMessage());
		Assertions.assertTrue(e.getMessage().contains("lf_chain"));
		Assertions.assertTrue(e.getMessage().contains("lf_change_log"));
	}

}
