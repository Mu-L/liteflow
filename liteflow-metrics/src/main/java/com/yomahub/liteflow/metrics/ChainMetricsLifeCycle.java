package com.yomahub.liteflow.metrics;

import com.yomahub.liteflow.lifecycle.PostProcessChainExecuteLifeCycle;
import com.yomahub.liteflow.slot.Slot;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * chain 级指标采集（执行次数/耗时/在途/错误）
 *
 * @author Bryan.Zhang
 */
public class ChainMetricsLifeCycle implements PostProcessChainExecuteLifeCycle {

    private final MeterRegistry registry;

    /** 每线程的样本栈，支持同线程嵌套子链（LIFO） */
    private static final ThreadLocal<Deque<ChainSample>> SAMPLES =
            ThreadLocal.withInitial(ArrayDeque::new);

    public ChainMetricsLifeCycle(MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void postProcessBeforeChainExecute(String chainId, Slot slot) {
        Timer.Sample timerSample = Timer.start(registry);
        LongTaskTimer.Sample activeSample = LongTaskTimer.builder("liteflow.chain.active")
                .tag("chain", chainId)
                .register(registry)
                .start();
        SAMPLES.get().push(new ChainSample(timerSample, activeSample));
    }

    @Override
    public void postProcessAfterChainExecute(String chainId, Slot slot) {
        Deque<ChainSample> stack = SAMPLES.get();
        ChainSample sample = stack.poll();
        try {
            if (sample == null) {
                return;
            }
            Exception ex = slot.getException();
            String status = (ex == null) ? "success" : "failed";

            sample.timerSample.stop(Timer.builder("liteflow.chain.executions")
                    .tag("chain", chainId)
                    .tag("status", status)
                    .register(registry));
            sample.activeSample.stop();

            if (ex != null) {
                registry.counter("liteflow.chain.errors",
                        "chain", chainId,
                        "exception", ex.getClass().getSimpleName()).increment();
            }
        } finally {
            if (stack.isEmpty()) {
                SAMPLES.remove();
            }
        }
    }

    private static final class ChainSample {
        final Timer.Sample timerSample;
        final LongTaskTimer.Sample activeSample;
        ChainSample(Timer.Sample timerSample, LongTaskTimer.Sample activeSample) {
            this.timerSample = timerSample;
            this.activeSample = activeSample;
        }
    }
}
