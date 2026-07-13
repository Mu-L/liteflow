package com.yomahub.liteflow.test.metrics;

import com.yomahub.liteflow.core.FlowExecutor;
import com.yomahub.liteflow.flow.LiteflowResponse;
import com.yomahub.liteflow.metrics.LiteflowMetaView;
import com.yomahub.liteflow.springboot4.metrics.LiteflowEndpoint;
import com.yomahub.liteflow.test.BaseTest;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.Resource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.Map;

@TestPropertySource(value = "classpath:/metrics/application.properties")
@SpringBootTest(classes = MetricsEndpointSpringboot4Test.class)
@EnableAutoConfiguration
@ComponentScan({ "com.yomahub.liteflow.test.metrics.cmp" })
public class MetricsEndpointSpringboot4Test extends BaseTest {

    @Resource
    private FlowExecutor flowExecutor;

    @Resource
    private MeterRegistry meterRegistry;

    @Resource
    private LiteflowMetaView liteflowMetaView;

    @Resource
    private LiteflowEndpoint liteflowEndpoint;

    @Test
    public void testChainAndNodeMetricsRecorded() {
        LiteflowResponse response = flowExecutor.execute2Resp("mChain", "arg");
        Assertions.assertTrue(response.isSuccess());

        Assertions.assertEquals(1,
                meterRegistry.get("liteflow.chain.executions")
                        .tags("chain", "mChain", "status", "success").timer().count());
        Assertions.assertEquals(1,
                meterRegistry.get("liteflow.node.executions")
                        .tags("node", "a", "status", "success").timer().count());
    }

    @Test
    public void testMetaViewStructure() {
        List<Map<String, Object>> chains = liteflowMetaView.chains();
        boolean found = chains.stream().anyMatch(c -> "mChain".equals(c.get("chainId")));
        Assertions.assertTrue(found);
    }

    @Test
    public void testInactiveRuleDbEndpointView() {
        Map<String, Object> view = (Map<String, Object>) liteflowEndpoint.chains("ruledb");
        Assertions.assertEquals(Boolean.FALSE, view.get("active"));
        Assertions.assertEquals(1, view.size());
    }
}
