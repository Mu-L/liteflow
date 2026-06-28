package com.yomahub.liteflow.metrics;

import com.yomahub.liteflow.core.NodeComponent;
import com.yomahub.liteflow.lifecycle.PostProcessNodeExecuteLifeCycle;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;

/**
 * node 级指标采集（执行次数/耗时/在途/错误）
 *
 * @author Bryan.Zhang
 */
public class NodeMetricsLifeCycle implements PostProcessNodeExecuteLifeCycle {

    private final MeterRegistry registry;

    /** 每线程在途样本栈（仅用于 active LongTaskTimer，LIFO） */
    private static final ThreadLocal<Deque<LongTaskTimer.Sample>> ACTIVE_SAMPLES =
            ThreadLocal.withInitial(ArrayDeque::new);

    public NodeMetricsLifeCycle(MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void postProcessBeforeNodeExecute(NodeComponent cmp) {
        LongTaskTimer.Sample activeSample = LongTaskTimer.builder("liteflow.node.active")
                .tag("node", nodeId(cmp))
                .register(registry)
                .start();
        ACTIVE_SAMPLES.get().push(activeSample);
    }

    @Override
    public void postProcessAfterNodeExecute(NodeComponent cmp, long timeSpent, Exception e) {
        Deque<LongTaskTimer.Sample> stack = ACTIVE_SAMPLES.get();
        LongTaskTimer.Sample activeSample = stack.poll();
        try {
            String node = nodeId(cmp);
            String type = (cmp.getType() == null) ? "UNKNOWN" : cmp.getType().name();
            String status = (e == null) ? "success" : "failed";

            Timer.builder("liteflow.node.executions")
                    .tag("node", node)
                    .tag("type", type)
                    .tag("status", status)
                    .register(registry)
                    .record(timeSpent, TimeUnit.MILLISECONDS);

            if (activeSample != null) {
                activeSample.stop();
            }

            if (e != null) {
                registry.counter("liteflow.node.errors",
                        "node", node,
                        "exception", e.getClass().getSimpleName()).increment();
            }
        } finally {
            if (stack.isEmpty()) {
                ACTIVE_SAMPLES.remove();
            }
        }
    }

    private static String nodeId(NodeComponent cmp) {
        String id = cmp.getNodeId();
        return (id == null) ? "unknown" : id;
    }
}
