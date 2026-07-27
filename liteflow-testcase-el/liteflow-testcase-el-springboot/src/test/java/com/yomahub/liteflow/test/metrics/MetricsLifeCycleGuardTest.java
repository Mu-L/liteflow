package com.yomahub.liteflow.test.metrics;

import com.yomahub.liteflow.core.NodeComponent;
import com.yomahub.liteflow.enums.NodeTypeEnum;
import com.yomahub.liteflow.metrics.ChainMetricsLifeCycle;
import com.yomahub.liteflow.metrics.NodeMetricsLifeCycle;
import com.yomahub.liteflow.slot.Slot;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * 指标钩子的健壮性单元测试（不依赖 Spring 容器）：
 * 1. 注册表/MeterFilter 异常不得从钩子中抛出（观测不能影响业务执行）
 * 2. before 样本缺失时，错误计数依然要记录（chain 与 node 行为一致）
 */
public class MetricsLifeCycleGuardTest {

    @Test
    public void testChainErrorCounterRecordedEvenWithoutBeforeSample() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ChainMetricsLifeCycle lifeCycle = new ChainMetricsLifeCycle(registry);

        Slot slot = new Slot();
        slot.setChainId("cX");
        slot.setException(new IllegalStateException("boom"));

        // 未调用 before，直接调用 after：错误计数不能丢
        lifeCycle.postProcessAfterChainExecute("cX", slot);

        Counter counter = registry.find("liteflow.chain.errors").tag("chain", "cX").counter();
        Assertions.assertNotNull(counter);
        Assertions.assertEquals(1.0, counter.count());
    }

    @Test
    public void testNodeErrorCounterRecordedEvenWithoutBeforeSample() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        NodeMetricsLifeCycle lifeCycle = new NodeMetricsLifeCycle(registry);

        NodeComponent cmp = bareCmp("nX");

        lifeCycle.postProcessAfterNodeExecute(cmp, 0, new IllegalStateException("boom"));

        Counter counter = registry.find("liteflow.node.errors").tag("node", "nX").counter();
        Assertions.assertNotNull(counter);
        Assertions.assertEquals(1.0, counter.count());
    }

    @Test
    public void testChainHookNeverPropagatesRegistryException() {
        SimpleMeterRegistry registry = throwingRegistry();
        ChainMetricsLifeCycle lifeCycle = new ChainMetricsLifeCycle(registry);

        Slot okSlot = new Slot();
        Slot errSlot = new Slot();
        errSlot.setChainId("cY");
        errSlot.setException(new IllegalStateException("boom"));

        Assertions.assertDoesNotThrow(() -> {
            lifeCycle.postProcessBeforeChainExecute("cY", okSlot);
            lifeCycle.postProcessAfterChainExecute("cY", errSlot);
        });
    }

    @Test
    public void testNodeHookNeverPropagatesRegistryException() {
        SimpleMeterRegistry registry = throwingRegistry();
        NodeMetricsLifeCycle lifeCycle = new NodeMetricsLifeCycle(registry);

        NodeComponent cmp = bareCmp("nY");

        Assertions.assertDoesNotThrow(() -> {
            lifeCycle.postProcessBeforeNodeExecute(cmp);
            lifeCycle.postProcessAfterNodeExecute(cmp, 0, new IllegalStateException("boom"));
        });
    }

    @Test
    public void testAfterOnEmptyStackIsHarmless() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ChainMetricsLifeCycle chainLc = new ChainMetricsLifeCycle(registry);
        NodeMetricsLifeCycle nodeLc = new NodeMetricsLifeCycle(registry);

        Slot slot = new Slot();
        slot.setChainId("cZ");
        NodeComponent cmp = bareCmp("nZ");

        Assertions.assertDoesNotThrow(() -> {
            chainLc.postProcessAfterChainExecute("cZ", slot);
            chainLc.postProcessAfterChainExecute("cZ", slot);
            nodeLc.postProcessAfterNodeExecute(cmp, 0, null);
            nodeLc.postProcessAfterNodeExecute(cmp, 0, null);
        });
    }

    /** 模拟用户 MeterFilter 行为异常导致 meter 注册抛错的注册表 */
    private SimpleMeterRegistry throwingRegistry() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        registry.config().meterFilter(new MeterFilter() {
            @Override
            public Meter.Id map(Meter.Id id) {
                throw new RuntimeException("meter filter boom");
            }
        });
        return registry;
    }

    private NodeComponent bareCmp(String nodeId) {
        NodeComponent cmp = new NodeComponent() {
            @Override
            public void process() {
            }
        };
        cmp.setNodeId(nodeId);
        cmp.setType(NodeTypeEnum.COMMON);
        return cmp;
    }
}
