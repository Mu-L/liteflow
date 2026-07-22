package com.yomahub.liteflow.test.ruledb;

import com.yomahub.liteflow.core.FlowExecutor;
import com.yomahub.liteflow.flow.LiteflowResponse;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.slot.DefaultContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * 脚本类型元数据回归：影子 Node 的 NodeTypeEnum 来自 manifest 的 script_type，
 * EL 编译期在没有脚本源码的情况下也必须知道节点类型（IF 需要布尔节点、SWITCH 需要选择节点），
 * 这正是 RuleManifest 携带 type/language/name 的设计动机（spec §5）。
 */
public class RuleDbScriptTypeTest extends BaseRuleDbTest {

	@Test
	public void testBooleanScriptDrivesIfCondition() {
		InMemoryRuleRepository.putScript("bs1", "return true", "boolean_script", "groovy");
		InMemoryRuleRepository.putChain("chainIf", "IF(bs1, a, b)");
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());

		LiteflowResponse r = executor.execute2Resp("chainIf", "arg");
		Assertions.assertTrue(r.isSuccess());
		Assertions.assertTrue(r.getExecuteStepStr().endsWith("a"),
				"boolean script returned true, expected the true-branch [a], steps: " + r.getExecuteStepStr());
	}

	@Test
	public void testSwitchScriptRoutesToTarget() {
		InMemoryRuleRepository.putScript("sw1", "return \"b\"", "switch_script", "groovy");
		InMemoryRuleRepository.putChain("chainSwitch", "SWITCH(sw1).to(a, b)");
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());

		LiteflowResponse r = executor.execute2Resp("chainSwitch", "arg");
		Assertions.assertTrue(r.isSuccess());
		Assertions.assertTrue(r.getExecuteStepStr().endsWith("b"),
				"switch script returned \"b\", expected route to [b], steps: " + r.getExecuteStepStr());
	}

	@Test
	public void testScriptWithoutLanguageUsesDefault() {
		// spec §6.1：script_language 为空时用 SPI 首个执行器作为全局默认
		// （本模块 pom 中 groovy 声明在 javax-pro 之前，故默认引擎仍是 groovy）
		InMemoryRuleRepository.putScript("sd1", "defaultContext.setData(\"sd1\", true);", "script", null);
		InMemoryRuleRepository.putChain("chainDefaultLang", "THEN(a, sd1)");
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());

		LiteflowResponse r = executor.execute2Resp("chainDefaultLang", "arg");
		Assertions.assertTrue(r.isSuccess());
		Assertions.assertEquals(Boolean.TRUE, r.getContextBean(DefaultContext.class).getData("sd1"));
	}

}
