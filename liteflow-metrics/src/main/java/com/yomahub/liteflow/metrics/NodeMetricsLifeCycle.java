package com.yomahub.liteflow.metrics;

import com.yomahub.liteflow.core.NodeComponent;
import com.yomahub.liteflow.lifecycle.PostProcessNodeExecuteLifeCycle;
import com.yomahub.liteflow.log.LFLog;
import com.yomahub.liteflow.log.LFLoggerManager;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * node 级指标采集（执行次数/耗时/在途/错误）
 *
 * <p>耗时采用 Micrometer 的 {@link Timer.Sample}（纳秒精度）自行计时，而不是依赖上游传入的
 * {@code timeSpent}——后者来自 {@code StopWatch.getTotalTimeMillis()}，对执行不足 1ms 的节点
 * 会被整除截断为 0，导致 meanMs/maxMs 永远为 0。与 {@link ChainMetricsLifeCycle} 保持一致。
 *
 * <p>meter 实例按 tag 组合缓存，避免热路径上每次执行重复构建 Meter.Id 并查找注册表。
 * 钩子体整体自兜底：注册表/MeterFilter 抛出的异常只记 debug 日志，不得影响节点执行
 * （after 钩子位于核心的 finally 中，抛错会吞掉业务异常）。
 *
 * @author Bryan.Zhang
 */
public class NodeMetricsLifeCycle implements PostProcessNodeExecuteLifeCycle {

    private static final LFLog LOG = LFLoggerManager.getLogger(NodeMetricsLifeCycle.class);

    private final MeterRegistry registry;

    private final Map<String, LongTaskTimer> activeTimers = new ConcurrentHashMap<>();

    private final Map<String, Timer> executionTimers = new ConcurrentHashMap<>();

    private final Map<String, Counter> errorCounters = new ConcurrentHashMap<>();

    /** 每线程的样本栈，支持同线程嵌套执行（LIFO） */
    private static final ThreadLocal<Deque<NodeSample>> SAMPLES =
            ThreadLocal.withInitial(ArrayDeque::new);

    public NodeMetricsLifeCycle(MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void postProcessBeforeNodeExecute(NodeComponent cmp) {
        Timer.Sample timerSample = null;
        LongTaskTimer.Sample activeSample = null;
        try {
            timerSample = Timer.start(registry);
            activeSample = activeTimer(nodeId(cmp)).start();
        }
        catch (Exception e) {
            LOG.debug("node metrics before-hook failed, metrics of this execution will be lost", e);
        }
        finally {
            // 即使采集失败也入栈占位，保证嵌套场景下 after 的 LIFO 配对不错位
            SAMPLES.get().push(new NodeSample(timerSample, activeSample));
        }
    }

    @Override
    public void postProcessAfterNodeExecute(NodeComponent cmp, long timeSpent, Exception e) {
        Deque<NodeSample> stack = SAMPLES.get();
        NodeSample sample = stack.poll();
        try {
            String node = nodeId(cmp);

            if (sample != null) {
                String type = (cmp.getType() == null) ? "UNKNOWN" : cmp.getType().name();
                String status = (e == null) ? "success" : "failed";
                if (sample.timerSample != null) {
                    sample.timerSample.stop(executionTimer(node, type, status));
                }
                if (sample.activeSample != null) {
                    sample.activeSample.stop();
                }
            }

            if (e != null) {
                errorCounter(node, e.getClass().getSimpleName()).increment();
            }
        }
        catch (Exception ex) {
            LOG.debug("node metrics after-hook failed, metrics of this execution will be lost", ex);
        }
        finally {
            if (stack.isEmpty()) {
                SAMPLES.remove();
            }
        }
    }

    private LongTaskTimer activeTimer(String nodeId) {
        return activeTimers.computeIfAbsent(nodeId,
                id -> LongTaskTimer.builder("liteflow.node.active")
                        .tag("node", id)
                        .register(registry));
    }

    private Timer executionTimer(String nodeId, String type, String status) {
        return executionTimers.computeIfAbsent(nodeId + '|' + type + '|' + status,
                k -> Timer.builder("liteflow.node.executions")
                        .tag("node", nodeId)
                        .tag("type", type)
                        .tag("status", status)
                        .register(registry));
    }

    private Counter errorCounter(String nodeId, String exception) {
        return errorCounters.computeIfAbsent(nodeId + '|' + exception,
                k -> registry.counter("liteflow.node.errors",
                        "node", nodeId,
                        "exception", exception));
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
