package com.yomahub.liteflow.test.ruledb;

import com.yomahub.liteflow.builder.LiteFlowNodeBuilder;
import com.yomahub.liteflow.core.FlowExecutor;
import com.yomahub.liteflow.core.FlowExecutorHolder;
import com.yomahub.liteflow.flow.FlowBus;
import com.yomahub.liteflow.property.LiteflowConfig;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.repository.RuleRepositoryHolder;
import com.yomahub.liteflow.test.ruledb.cmp.ACmp;
import com.yomahub.liteflow.test.ruledb.cmp.BCmp;
import com.yomahub.liteflow.test.ruledb.cmp.CCmp;
import org.junit.jupiter.api.AfterEach;

import java.util.function.Supplier;

public abstract class BaseRuleDbTest {

    @AfterEach
    public void cleanup() {
        // TODO restore in Task 3 (RuleDbRuntime not yet created)
        // RuleDbRuntime.destroy();
        // TODO Task 3: import RuleDbRuntime
        RuleRepositoryHolder.reset();
        FlowBus.cleanCache();
        FlowBus.clearStat();
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
