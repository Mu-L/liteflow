package com.yomahub.liteflow.test.ruledb;

import com.yomahub.liteflow.core.FlowExecutor;
import com.yomahub.liteflow.flow.LiteflowResponse;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.slot.DefaultContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * rule-db 模式 + javax-pro 脚本引擎的启动相位回归。
 *
 * 背景：FlowExecutor.init 的 rule-db 分支曾经提前 return，既未执行 FlowInitHook，
 * 也未把 startUpPhase 复位为 false。javax-pro 的 JavaxProExecutor 是唯一感知
 * startUpPhase 的脚本执行器：startUpPhase=true 时 load 只把 CodeSpec 暂存进
 * codeSpecMap（等待 loadSecondPhase 批量编译），不写入 compiledScriptMap。
 * 于是 rule-db 在运行期懒加载脚本时永远走启动期缓冲分支，而 loadSecondPhase
 * 又不会在运行期触发，最终执行报 "script for node[...] is not loaded"。
 */
public class RuleDbJavaxProStartupPhaseTest extends BaseRuleDbTest {

	private static final String JAVA_SCRIPT =
			"import com.yomahub.liteflow.core.NodeComponent;\n"
			+ "import com.yomahub.liteflow.slot.DefaultContext;\n"
			+ "public class Demo extends NodeComponent {\n"
			+ "    @Override\n"
			+ "    public void process() throws Exception {\n"
			+ "        DefaultContext ctx = this.getFirstContextBean();\n"
			+ "        ctx.setData(\"s1\", \"javax-pro-ok\");\n"
			+ "    }\n"
			+ "}\n";

	@Test
	public void testStartUpPhaseResetAfterRuleDbInit() {
		InMemoryRuleRepository.putChain("chain1", "THEN(a, b)");
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());

		// 直接断言被修复的不变量：rule-db 分支走完后 startUpPhase 必须复位
		Assertions.assertFalse(executor.getStartUpPhase().get());
	}

	@Test
	public void testLazyLoadedJavaxProScriptExecutes() {
		InMemoryRuleRepository.putScript("s1", JAVA_SCRIPT, "script", "java");
		InMemoryRuleRepository.putChain("chain1", "THEN(a, s1)");
		registerCommonCmp();
		FlowExecutor executor = buildExecutor(new RuleDbConfig());

		// 运行期懒加载并编译 javax-pro 脚本；修复前此处必报
		// "script for node[s1] is not loaded"
		LiteflowResponse r = executor.execute2Resp("chain1", "arg");
		Assertions.assertTrue(r.isSuccess(),
				"expected success but got: " + (r.getCause() == null ? "null" : r.getCause().getMessage()));
		Assertions.assertEquals("javax-pro-ok", r.getContextBean(DefaultContext.class).getData("s1"));
	}

	@Test
	public void testPreloadedJavaxProScriptCompiledByInitHook() {
		InMemoryRuleRepository.putScript("s1", JAVA_SCRIPT, "script", "java");
		InMemoryRuleRepository.putChain("chainP", "THEN(a, s1)");
		registerCommonCmp();
		RuleDbConfig cfg = new RuleDbConfig();
		cfg.getCache().setPreloadChainIds("chainP");
		FlowExecutor executor = buildExecutor(cfg);

		// 预热发生在 startUpPhase=true 期间，脚本被暂存进 codeSpecMap；
		// rule-db 分支补上的 FlowInitHook.executeHook() 必须触发 loadSecondPhase 批量编译
		LiteflowResponse r = executor.execute2Resp("chainP", "arg");
		Assertions.assertTrue(r.isSuccess(),
				"expected success but got: " + (r.getCause() == null ? "null" : r.getCause().getMessage()));
		Assertions.assertEquals("javax-pro-ok", r.getContextBean(DefaultContext.class).getData("s1"));
	}
}
