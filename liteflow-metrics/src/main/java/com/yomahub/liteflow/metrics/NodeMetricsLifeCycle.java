package com.yomahub.liteflow.metrics;

import com.yomahub.liteflow.core.NodeComponent;
import com.yomahub.liteflow.lifecycle.PostProcessNodeExecuteLifeCycle;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * node 级指标采集（执行次数/耗时/在途/错误）
 *
 * <p>耗时采用 Micrometer 的 {@link Timer.Sample}（纳秒精度）自行计时，而不是依赖上游传入的
 * {@code timeSpent}——后者来自 {@code StopWatch.getTotalTimeMillis()}，对执行不足 1ms 的节点
 * 会被整除截断为 0，导致 meanMs/maxMs 永远为 0。与 {@link ChainMetricsLifeCycle} 保持一致。
 *
 * @author Bryan.Zhang
 */
public class NodeMetricsLifeCycle implements PostProcessNodeExecuteLifeCycle {

    private final MeterRegistry registry;

    /** 每线程的样本栈，支持同线程嵌套执行（LIFO） */
    private static final ThreadLocal<Deque<NodeSample>> SAMPLES =
            ThreadLocal.withInitial(ArrayDeque::new);

    public NodeMetricsLifeCycle(MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void postProcessBeforeNodeExecute(NodeComponent cmp) {
        Timer.Sample timerSample = Timer.start(registry);
        LongTaskTimer.Sample activeSample = LongTaskTimer.builder("liteflow.node.active")
                .tag("node", nodeId(cmp))
                .register(registry)
                .start();
        SAMPLES.get().push(new NodeSample(timerSample, activeSample));
    }

    @Override
    public void postProcessAfterNodeExecute(NodeComponent cmp, long timeSpent, Exception e) {
        Deque<NodeSample> stack = SAMPLES.get();
        NodeSample sample = stack.poll();
        try {
            String node = nodeId(cmp);
            String type = (cmp.getType() == null) ? "UNKNOWN" : cmp.getType().name();
            String status = (e == null) ? "success" : "failed";

            if (sample != null) {
                sample.timerSample.stop(Timer.builder("liteflow.node.executions")
                        .tag("node", node)
                        .tag("type", type)
                        .tag("status", status)
                        .register(registry));
                sample.activeSample.stop();
            }

            if (e != null) {
                registry.counter("liteflow.node.errors",
                        "node", node,
                        "exception", e.getClass().getSimpleName()).increment();
            }
        } finally {
            if (stack.isEmpty()) {
                SAMPLES.remove();
            }
        }
    }

    private static String nodeId(NodeComponent cmp) {
        String id = cmp.getNodeId();
        return (id == null) ? "unknown" : id;
    }

    private static final class NodeSample {
        final Timer.Sample timerSample;
        final LongTaskTimer.Sample activeSample;
        NodeSample(Timer.Sample timerSample, LongTaskTimer.Sample activeSample) {
            this.timerSample = timerSample;
            this.activeSample = activeSample;
        }
    }
}
