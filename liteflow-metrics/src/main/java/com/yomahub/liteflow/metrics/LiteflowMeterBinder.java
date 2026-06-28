package com.yomahub.liteflow.metrics;

import com.yomahub.liteflow.flow.FlowBus;
import com.yomahub.liteflow.property.LiteflowConfig;
import com.yomahub.liteflow.slot.DataBus;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;

/**
 * LiteFlow 注册表 / slot 池的 Gauge 绑定
 *
 * @author Bryan.Zhang
 */
public class LiteflowMeterBinder implements MeterBinder {

    private final LiteflowConfig liteflowConfig;

    public LiteflowMeterBinder(LiteflowConfig liteflowConfig) {
        this.liteflowConfig = liteflowConfig;
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        Gauge.builder("liteflow.chains.registered", FlowBus.getChainMap(), m -> m.size())
                .description("已注册的 chain 数量")
                .register(registry);

        Gauge.builder("liteflow.nodes.registered", FlowBus.getNodeMap(), m -> m.size())
                .description("已注册的 node 数量")
                .register(registry);

        Gauge.builder("liteflow.slot.size", liteflowConfig, c -> {
                    Integer s = c.getSlotSize();
                    return s == null ? 0 : s;
                })
                .description("slot 池容量")
                .register(registry);

        Gauge.builder("liteflow.slot.occupied", DataBus.OCCUPY_COUNT, java.util.concurrent.atomic.AtomicInteger::get)
                .description("当前在用 slot 数")
                .register(registry);
    }
}
