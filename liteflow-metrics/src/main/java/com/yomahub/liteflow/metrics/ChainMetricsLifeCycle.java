package com.yomahub.liteflow.metrics;

import com.yomahub.liteflow.lifecycle.PostProcessChainExecuteLifeCycle;
import com.yomahub.liteflow.log.LFLog;
import com.yomahub.liteflow.log.LFLoggerManager;
import com.yomahub.liteflow.slot.Slot;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * chain 级指标采集（执行次数/耗时/在途/错误）
 *
 * <p>meter 实例按 tag 组合缓存，避免热路径上每次执行重复构建 Meter.Id 并查找注册表。
 * 钩子体整体自兜底：注册表/MeterFilter 抛出的异常只记 debug 日志，不得影响业务链执行
 * （chain 级 before 钩子在核心中没有防御性包装，after 钩子位于 finally 中，抛错会吞掉业务异常）。
 *
 * @author Bryan.Zhang
 */
public class ChainMetricsLifeCycle implements PostProcessChainExecuteLifeCycle {

    private static final LFLog LOG = LFLoggerManager.getLogger(ChainMetricsLifeCycle.class);

    private static final String SCOPE_MAIN = "main";

    private static final String SCOPE_SUB = "sub";

    private final MeterRegistry registry;

    private final Map<String, LongTaskTimer> activeTimers = new ConcurrentHashMap<>();

    private final Map<String, Timer> executionTimers = new ConcurrentHashMap<>();

    private final Map<String, Counter> errorCounters = new ConcurrentHashMap<>();

    /** 每线程的样本栈，支持同线程嵌套子链（LIFO） */
    private static final ThreadLocal<Deque<ChainSample>> SAMPLES =
            ThreadLocal.withInitial(ArrayDeque::new);

    public ChainMetricsLifeCycle(MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void postProcessBeforeChainExecute(String chainId, Slot slot) {
        Timer.Sample timerSample = null;
        LongTaskTimer.Sample activeSample = null;
        String scope = SCOPE_MAIN;
        try {
            // slot 的 chainId 只在首个 chain 执行时写入一次，此刻尚未写入即为主链
            scope = (slot == null || slot.getChainId() == null) ? SCOPE_MAIN : SCOPE_SUB;
            timerSample = Timer.start(registry);
            activeSample = activeTimer(chainId).start();
        }
        catch (Exception e) {
            LOG.debug("chain metrics before-hook failed, metrics of this execution will be lost", e);
        }
        finally {
            // 即使采集失败也入栈占位，保证嵌套场景下 after 的 LIFO 配对不错位
            SAMPLES.get().push(new ChainSample(timerSample, activeSample, scope));
        }
    }

    @Override
    public void postProcessAfterChainExecute(String chainId, Slot slot) {
        Deque<ChainSample> stack = SAMPLES.get();
        ChainSample sample = stack.poll();
        try {
            Exception ex = (slot == null) ? null : slot.getException();
            if (sample != null) {
                String status = (ex == null) ? "success" : "failed";
                if (sample.timerSample != null) {
                    sample.timerSample.stop(executionTimer(chainId, sample.scope, status));
                }
                if (sample.activeSample != null) {
                    sample.activeSample.stop();
                }
            }
            if (ex != null) {
                errorCounter(chainId, ex.getClass().getSimpleName()).increment();
            }
        }
        catch (Exception e) {
            LOG.debug("chain metrics after-hook failed, metrics of this execution will be lost", e);
        }
        finally {
            if (stack.isEmpty()) {
                SAMPLES.remove();
            }
        }
    }

    private LongTaskTimer activeTimer(String chainId) {
        return activeTimers.computeIfAbsent(chainId,
                id -> LongTaskTimer.builder("liteflow.chain.active")
                        .tag("chain", id)
                        .register(registry));
    }

    private Timer executionTimer(String chainId, String scope, String status) {
        return executionTimers.computeIfAbsent(chainId + '|' + scope + '|' + status,
                k -> Timer.builder("liteflow.chain.executions")
                        .tag("chain", chainId)
                        .tag("scope", scope)
                        .tag("status", status)
                        .register(registry));
    }

    private Counter errorCounter(String chainId, String exception) {
        return errorCounters.computeIfAbsent(chainId + '|' + exception,
                k -> registry.counter("liteflow.chain.errors",
                        "chain", chainId,
                        "exception", exception));
    }

    private static final class ChainSample {
        final Timer.Sample timerSample;
        final LongTaskTimer.Sample activeSample;
        final String scope;
        ChainSample(Timer.Sample timerSample, LongTaskTimer.Sample activeSample, String scope) {
            this.timerSample = timerSample;
            this.activeSample = activeSample;
            this.scope = scope;
        }
    }
}
