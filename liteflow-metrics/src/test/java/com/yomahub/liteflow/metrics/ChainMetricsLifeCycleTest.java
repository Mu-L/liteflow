package com.yomahub.liteflow.metrics;

import com.yomahub.liteflow.slot.Slot;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ChainMetricsLifeCycleTest {

    @Test
    public void testSuccessTimer() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ChainMetricsLifeCycle lc = new ChainMetricsLifeCycle(registry);

        Slot slot = new Slot();
        lc.postProcessBeforeChainExecute("cA", slot);
        lc.postProcessAfterChainExecute("cA", slot);

        Assertions.assertEquals(1,
                registry.get("liteflow.chain.executions")
                        .tags("chain", "cA", "status", "success").timer().count());
    }

    @Test
    public void testFailureTimerAndErrorCounter() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ChainMetricsLifeCycle lc = new ChainMetricsLifeCycle(registry);

        Slot slot = new Slot();
        slot.setException(new IllegalArgumentException("bad"));
        lc.postProcessBeforeChainExecute("cB", slot);
        lc.postProcessAfterChainExecute("cB", slot);

        Assertions.assertEquals(1,
                registry.get("liteflow.chain.executions")
                        .tags("chain", "cB", "status", "failed").timer().count());
        Assertions.assertEquals(1.0,
                registry.get("liteflow.chain.errors")
                        .tags("chain", "cB", "exception", "IllegalArgumentException").counter().count());
    }

    @Test
    public void testNestedChainsLifo() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ChainMetricsLifeCycle lc = new ChainMetricsLifeCycle(registry);

        Slot slot = new Slot();
        // before outer → before inner → after inner → after outer
        lc.postProcessBeforeChainExecute("outer", slot);
        lc.postProcessBeforeChainExecute("inner", slot);
        lc.postProcessAfterChainExecute("inner", slot);
        lc.postProcessAfterChainExecute("outer", slot);

        Assertions.assertEquals(1,
                registry.get("liteflow.chain.executions").tags("chain", "inner", "status", "success").timer().count());
        Assertions.assertEquals(1,
                registry.get("liteflow.chain.executions").tags("chain", "outer", "status", "success").timer().count());
    }
}
