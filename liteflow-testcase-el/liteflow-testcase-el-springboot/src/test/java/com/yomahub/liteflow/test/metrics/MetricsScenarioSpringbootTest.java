package com.yomahub.liteflow.test.metrics;

import com.yomahub.liteflow.core.FlowExecutor;
import com.yomahub.liteflow.flow.LiteflowResponse;
import com.yomahub.liteflow.test.BaseTest;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.TestPropertySource;

import javax.annotation.Resource;

/**
 * 指标采集的场景测试：失败路径、CATCH 语义、嵌套链的 scope 维度、meter 复用累加。
 * 各场景使用独立的 chain / node，避免跨用例的计数污染。
 */
@TestPropertySource(value = "classpath:/metrics-scenario/application.properties")
@SpringBootTest(classes = MetricsScenarioSpringbootTest.class)
@EnableAutoConfiguration
@ComponentScan({ "com.yomahub.liteflow.test.metrics.cmp" })
public class MetricsScenarioSpringbootTest extends BaseTest {

    @Resource
    private FlowExecutor flowExecutor;

    @Resource
    private MeterRegistry meterRegistry;

    @Test
    public void testFailedNodeAndChainMetrics() {
        LiteflowResponse response = flowExecutor.execute2Resp("errChain", "arg");
        Assertions.assertFalse(response.isSuccess());

        Assertions.assertEquals(1,
                meterRegistry.get("liteflow.node.executions")
                        .tags("node", "e", "status", "failed").timer().count());
        Assertions.assertEquals(1,
                meterRegistry.get("liteflow.chain.executions")
                        .tags("chain", "errChain", "status", "failed").timer().count());

        Counter nodeErrors = meterRegistry.find("liteflow.node.errors").tag("node", "e").counter();
        Assertions.assertNotNull(nodeErrors);
        Assertions.assertEquals(1.0, nodeErrors.count());

        Counter chainErrors = meterRegistry.find("liteflow.chain.errors").tag("chain", "errChain").counter();
        Assertions.assertNotNull(chainErrors);
        Assertions.assertEquals(1.0, chainErrors.count());
    }

    @Test
    public void testCatchedChainCountsAsSuccess() {
        LiteflowResponse response = flowExecutor.execute2Resp("catchChain", "arg");
        Assertions.assertTrue(response.isSuccess());

        // 被 CATCH 兜住后，chain 级记 success，且不产生 chain 级错误计数
        Assertions.assertEquals(1,
                meterRegistry.get("liteflow.chain.executions")
                        .tags("chain", "catchChain", "status", "success").timer().count());
        Assertions.assertNull(
                meterRegistry.find("liteflow.chain.errors").tag("chain", "catchChain").counter());

        // node 级失败仍然如实记录
        Assertions.assertEquals(1,
                meterRegistry.get("liteflow.node.executions")
                        .tags("node", "f", "status", "failed").timer().count());
    }

    @Test
    public void testNestedChainScopeTag() {
        LiteflowResponse response = flowExecutor.execute2Resp("mainChain", "arg");
        Assertions.assertTrue(response.isSuccess());

        // 主链与子链各计一次，通过 scope 维度区分
        Assertions.assertEquals(1,
                meterRegistry.get("liteflow.chain.executions")
                        .tags("chain", "mainChain", "scope", "main", "status", "success").timer().count());
        Assertions.assertEquals(1,
                meterRegistry.get("liteflow.chain.executions")
                        .tags("chain", "subChain", "scope", "sub", "status", "success").timer().count());
    }

    @Test
    public void testMeterAccumulatesAcrossExecutions() {
        Assertions.assertTrue(flowExecutor.execute2Resp("okChain", "arg").isSuccess());
        Assertions.assertTrue(flowExecutor.execute2Resp("okChain", "arg").isSuccess());

        // 多次执行累加在同一个 meter 上（守护 meter 缓存实现不产生新序列）
        Assertions.assertEquals(2,
                meterRegistry.get("liteflow.chain.executions")
                        .tags("chain", "okChain", "status", "success").timer().count());
        Assertions.assertEquals(2,
                meterRegistry.get("liteflow.node.executions")
                        .tags("node", "g", "status", "success").timer().count());
    }
}
