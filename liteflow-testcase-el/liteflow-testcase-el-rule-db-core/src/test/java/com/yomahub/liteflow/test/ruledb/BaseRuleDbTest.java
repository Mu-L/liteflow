package com.yomahub.liteflow.test.ruledb;

import com.yomahub.liteflow.builder.LiteFlowNodeBuilder;
import com.yomahub.liteflow.core.FlowExecutor;
import com.yomahub.liteflow.core.FlowExecutorHolder;
import com.yomahub.liteflow.flow.FlowBus;
import com.yomahub.liteflow.property.LiteflowConfig;
import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.repository.RuleDbRuntime;
import com.yomahub.liteflow.repository.RuleRepositoryHolder;
import com.yomahub.liteflow.test.ruledb.cmp.ACmp;
import com.yomahub.liteflow.test.ruledb.cmp.BCmp;
import com.yomahub.liteflow.test.ruledb.cmp.CCmp;
import org.junit.jupiter.api.AfterEach;

import java.util.function.Supplier;

public abstract class BaseRuleDbTest {

    @AfterEach
    public void cleanup() {
        // 销毁 Rule-DB 运行时（索引/缓存态/initialized 标志），重置 SPI 解析，
        // 并清理 FlowExecutorHolder 单例与全局配置，避免多个测试类之间状态泄漏
        RuleDbRuntime.destroy();
        RuleRepositoryHolder.reset();
        FlowExecutorHolder.clean();
        FlowBus.cleanCache();
        FlowBus.clearStat();
        LiteflowConfigGetter.clean();
        InMemoryRuleRepository.reset();
    }

    /** 注册普通 Java 组件（rule-db 只纳管 EL 与脚本，Java 组件仍在 JVM 内） */
    protected void registerCommonCmp() {
        LiteFlowNodeBuilder.createCommonNode().setId("a").setClazz(ACmp.class).build();
        LiteFlowNodeBuilder.createCommonNode().setId("b").setClazz(BCmp.class).build();
        LiteFlowNodeBuilder.createCommonNode().setId("c").setClazz(CCmp.class).build();
    }

    protected FlowExecutor buildExecutor(RuleDbConfig ruleDb) {
        LiteflowConfig config = new LiteflowConfig();
        config.setRuleDb(ruleDb);
        return FlowExecutorHolder.loadInstance(config);
    }

    protected void waitUntil(Supplier<Boolean> condition, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (Boolean.TRUE.equals(condition.get())) {
                return;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        throw new AssertionError("condition not met within " + timeoutMs + "ms");
    }
}
