package com.yomahub.liteflow.metrics;

import com.yomahub.liteflow.flow.FlowBus;
import com.yomahub.liteflow.property.LiteflowConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class LiteflowMeterBinderTest {

    @Test
    public void testGauges() {
        LiteflowConfig config = new LiteflowConfig();
        config.setSlotSize(2048);

        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        new LiteflowMeterBinder(config).bindTo(registry);

        // slot.size 反映配置
        Assertions.assertEquals(2048.0,
                registry.get("liteflow.slot.size").gauge().value());

        // chains.registered 反映 FlowBus 当前大小
        double before = registry.get("liteflow.chains.registered").gauge().value();
        FlowBus.addChain("metricsTestChain");
        double after = registry.get("liteflow.chains.registered").gauge().value();
        Assertions.assertEquals(before + 1, after);

        // 其余两个 Gauge 存在
        Assertions.assertNotNull(registry.get("liteflow.nodes.registered").gauge());
        Assertions.assertNotNull(registry.get("liteflow.slot.occupied").gauge());
    }
}
