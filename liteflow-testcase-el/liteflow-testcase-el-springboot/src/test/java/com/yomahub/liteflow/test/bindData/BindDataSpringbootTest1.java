package com.yomahub.liteflow.test.bindData;

import com.yomahub.liteflow.core.FlowExecutor;
import com.yomahub.liteflow.flow.LiteflowResponse;
import com.yomahub.liteflow.flow.entity.CmpStep;
import com.yomahub.liteflow.slot.DefaultContext;
import com.yomahub.liteflow.test.BaseTest;
import com.yomahub.liteflow.util.JsonUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.TestPropertySource;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * springboot环境EL常规的例子测试
 *
 * @author Bryan.Zhang
 */
@TestPropertySource(value = "classpath:/bindData/application1.properties")
@SpringBootTest(classes = BindDataSpringbootTest1.class)
@EnableAutoConfiguration
@ComponentScan({ "com.yomahub.liteflow.test.bindData.cmp1" })
public class BindDataSpringbootTest1 extends BaseTest {

	@Resource
	private FlowExecutor flowExecutor;

	// 测试bind关键字,简单情况
	@Test
	public void testBind1() throws Exception {
		LiteflowResponse response = flowExecutor.execute2Resp("chain1", "arg");
		DefaultContext context = response.getFirstContextBean();
		Assertions.assertEquals("test", context.getData("a"));
		Assertions.assertNull(context.getData("b"));
		Assertions.assertTrue(response.isSuccess());
	}

	// 测试bind关键字,表达式上加
	@Test
	public void testBind2() throws Exception {
		LiteflowResponse response = flowExecutor.execute2Resp("chain2", "arg");
		DefaultContext context = response.getFirstContextBean();
		Assertions.assertEquals("test", context.getData("a"));
		Assertions.assertEquals("test", context.getData("b"));
		Assertions.assertTrue(response.isSuccess());
	}

	// 测试bind关键字,相对复杂情况
	@Test
	public void testBind3() throws Exception {
		LiteflowResponse response = flowExecutor.execute2Resp("chain3", "arg");
		DefaultContext context = response.getFirstContextBean();
		Assertions.assertEquals("test", context.getData("a"));
		Assertions.assertEquals("test", context.getData("b"));
		Assertions.assertEquals("test", context.getData("c"));
		Assertions.assertEquals("test", context.getData("d"));
		Assertions.assertEquals("test", context.getData("x"));
		Assertions.assertEquals("test", context.getData("y"));
		Assertions.assertTrue(response.isSuccess());
	}

	// 测试bind关键字,对一个chain进行bind
	@Test
	public void testBind4() throws Exception {
		LiteflowResponse response = flowExecutor.execute2Resp("chain4", "arg");
		DefaultContext context = response.getFirstContextBean();
		Assertions.assertEquals("test2", context.getData("a"));
		Assertions.assertEquals("test2", context.getData("x"));
		Assertions.assertEquals("test2", context.getData("c"));
		Assertions.assertTrue(response.isSuccess());
	}

	// 测试bind一个对象，并且对象中的birth类型为LocalDate
	@Test
	public void testBind5() throws Exception {
		LiteflowResponse response = flowExecutor.execute2Resp("chain5", "arg");
		DefaultContext context = response.getFirstContextBean();
		System.out.println(JsonUtil.toJsonString(context.getData("f")));
		Assertions.assertTrue(response.isSuccess());
	}

	// 看看能否覆盖绑定
	@Test
	public void testBind6() throws Exception {
		LiteflowResponse response = flowExecutor.execute2Resp("chain6", "arg");
		DefaultContext context = response.getFirstContextBean();
		Assertions.assertEquals("test", context.getData("a"));
		Assertions.assertEquals("test_b", context.getData("b"));
		Assertions.assertEquals("test", context.getData("c"));
		Assertions.assertTrue(response.isSuccess());
	}

	// 强制覆盖
	@Test
	public void testBind7() throws Exception {
		LiteflowResponse response = flowExecutor.execute2Resp("chain7", "arg");
		DefaultContext context = response.getFirstContextBean();
		Assertions.assertEquals("test", context.getData("a"));
		Assertions.assertEquals("test", context.getData("b"));
		Assertions.assertEquals("test", context.getData("c"));
		Assertions.assertTrue(response.isSuccess());
	}

	// 复现 issue #IJT2YA：表达式级 bind 的数据，执行结束后应能通过 Node#getBindData 读取到，
	// 且节点级 bind 不会被表达式级 bind 覆盖（a=v2, b=v1, c=v2）
	@Test
	public void testBind8() throws Exception {
		String el = "THEN(a, b.bind(\"k\", \"v1\"), c).bind(\"k\", \"v2\");";
		LiteflowResponse response = flowExecutor.execute2RespWithEL(el);
		Assertions.assertTrue(response.isSuccess());
		Map<String, String> resultMap = new HashMap<>();
		for (CmpStep cmpStep : response.getExecuteStepQueue()) {
			resultMap.put(cmpStep.getNodeId(), cmpStep.getRefNode().getBindData("k"));
		}
		Assertions.assertEquals("v2", resultMap.get("a"));
		Assertions.assertEquals("v1", resultMap.get("b"));
		Assertions.assertEquals("v2", resultMap.get("c"));
	}

	// 回归保护：表达式级 bind 作用于含子 chain 引用的条件时，绝不能污染被共享的子 chain（#ID7OTO）
	@Test
	public void testBind9() throws Exception {
		// 带 bind 的 chain：运行时子 chain（psub）内的节点 a 应解析到 outer
		LiteflowResponse responseC = flowExecutor.execute2Resp("pchainC", "arg");
		DefaultContext ctxC = responseC.getFirstContextBean();
		Assertions.assertTrue(responseC.isSuccess());
		Assertions.assertEquals("outer", ctxC.getData("a"));

		// 未 bind 的 chain 单独引用同一子 chain，绝不能被上面的 bind 污染
		LiteflowResponse responseB = flowExecutor.execute2Resp("pchainB", "arg");
		DefaultContext ctxB = responseB.getFirstContextBean();
		Assertions.assertTrue(responseB.isSuccess());
		Assertions.assertNull(ctxB.getData("a"));
	}

}
