package com.yomahub.liteflow.metrics;

import com.yomahub.liteflow.core.NodeComponent;
import com.yomahub.liteflow.enums.NodeTypeEnum;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

public class NodeMetricsLifeCycleTest {

    static class FakeCmp extends NodeComponent {
        @Override
        public void process() { }
    }

    private NodeComponent cmp(String nodeId) {
        NodeComponent c = new FakeCmp();
        c.setNodeId(nodeId);
        c.setType(NodeTypeEnum.COMMON);
        return c;
    }

    @Test
    public void testSuccessTimer() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        NodeMetricsLifeCycle lc = new NodeMetricsLifeCycle(registry);

        NodeComponent c = cmp("nA");
        lc.postProcessBeforeNodeExecute(c);
        lc.postProcessAfterNodeExecute(c, 12L, null);

        Assertions.assertEquals(1,
                registry.get("liteflow.node.executions")
                        .tags("node", "nA", "type", "COMMON", "status", "success").timer().count());
        Assertions.assertEquals(12.0,
                registry.get("liteflow.node.executions")
                        .tags("node", "nA", "type", "COMMON", "status", "success")
                        .timer().totalTime(TimeUnit.MILLISECONDS), 0.001);
    }

    @Test
    public void testFailureTimerAndErrorCounter() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        NodeMetricsLifeCycle lc = new NodeMetricsLifeCycle(registry);

        NodeComponent c = cmp("nB");
        lc.postProcessBeforeNodeExecute(c);
        lc.postProcessAfterNodeExecute(c, 5L, new IllegalStateException("boom"));

        Assertions.assertEquals(1,
                registry.get("liteflow.node.executions")
                        .tags("node", "nB", "type", "COMMON", "status", "failed").timer().count());
        Assertions.assertEquals(1.0,
                registry.get("liteflow.node.errors")
                        .tags("node", "nB", "exception", "IllegalStateException").counter().count());
    }
}
