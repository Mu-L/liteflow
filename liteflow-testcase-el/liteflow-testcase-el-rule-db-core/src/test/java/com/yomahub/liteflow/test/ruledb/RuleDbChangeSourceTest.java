package com.yomahub.liteflow.test.ruledb;

import com.yomahub.liteflow.core.FlowExecutor;
import com.yomahub.liteflow.flow.FlowBus;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.repository.RuleDbProviderHolder;
import com.yomahub.liteflow.repository.RuleDbRuntime;
import com.yomahub.liteflow.repository.RuleDbSyncManager;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class RuleDbChangeSourceTest extends BaseRuleDbTest {

    @Test
    void replaysEventArrivingBetweenOpenAndManifest() {
        InMemoryRuleDbProvider provider = provider();
        InMemoryRuleRepository.publishChain("chain1", "THEN(a, b)");
        provider.afterManifestSnapshot(() -> {
            InMemoryRuleRepository.publishChain("chain1", "THEN(b, a)");
            provider.emit(lastChange());
        });
        registerCommonCmp();

        FlowExecutor executor = buildExecutor(new RuleDbConfig());

        Assertions.assertEquals(2L, RuleDbRuntime.getChainVersion("chain1"));
        Assertions.assertEquals("b==>a",
                executor.execute2Resp("chain1", "arg").getExecuteStepStr());
    }

    @Test
    void closeMakesLateCallbackNoOp() {
        InMemoryRuleDbProvider provider = provider();
        buildExecutor(new RuleDbConfig());
        Assertions.assertEquals(1, provider.getOpenCalls());

        RuleDbRuntime.destroy();
        provider.emitLate(new ChangeRecord(3, ChangeRecord.TargetType.CHAIN,
                "late", ChangeRecord.Op.UPSERT, 1));

        Assertions.assertFalse(FlowBus.containChain("late"));
    }

    @Test
    void activateDropsBaselineDuplicatesAndAppliesBufferedChangesInOrder() {
        InMemoryRuleDbProvider provider = provider();
        InMemoryRuleRepository.publishChain("baseline", "THEN(a)");
        provider.afterManifestSnapshot(() -> {
            provider.emit(new ChangeRecord(3, ChangeRecord.TargetType.CHAIN,
                    "ordered", ChangeRecord.Op.UPSERT, 1));
            provider.emit(new ChangeRecord(1, ChangeRecord.TargetType.CHAIN,
                    "duplicate", ChangeRecord.Op.UPSERT, 1));
            provider.emit(new ChangeRecord(2, ChangeRecord.TargetType.CHAIN,
                    "ordered", ChangeRecord.Op.UPSERT, 2));
        });

        buildExecutor(new RuleDbConfig());

        Assertions.assertFalse(FlowBus.containChain("duplicate"));
        Assertions.assertEquals(2L, RuleDbRuntime.getChainVersion("ordered"));
    }

    @Test
    void sortsLiveBatchBeforeAdvancingCursor() {
        InMemoryRuleDbProvider provider = provider();
        buildExecutor(new RuleDbConfig());

        provider.emitBatch(Arrays.asList(
                new ChangeRecord(3, ChangeRecord.TargetType.CHAIN,
                        "ordered", ChangeRecord.Op.UPSERT, 1),
                new ChangeRecord(2, ChangeRecord.TargetType.CHAIN,
                        "ordered", ChangeRecord.Op.UPSERT, 2)));

        Assertions.assertEquals(2L, RuleDbRuntime.getChainVersion("ordered"));
    }

    @Test
    void reconcileRequestRefreshesManifest() {
        InMemoryRuleDbProvider provider = provider();
        InMemoryRuleRepository.publishChain("chain1", "THEN(a, b)");
        registerCommonCmp();
        FlowExecutor executor = buildExecutor(new RuleDbConfig());
        Assertions.assertEquals("a==>b",
                executor.execute2Resp("chain1", "arg").getExecuteStepStr());

        InMemoryRuleRepository.putChain("chain1", "THEN(b, a)");
        provider.requestReconcile();

        Assertions.assertEquals("b==>a",
                executor.execute2Resp("chain1", "arg").getExecuteStepStr());
    }

    @Test
    void startAndStopAreIdempotent() {
        InMemoryRuleDbProvider provider = provider();

        buildExecutor(new RuleDbConfig());
        RuleDbRuntime.init();
        RuleDbRuntime.init();
        RuleDbSyncManager.start();
        RuleDbSyncManager.start();

        Assertions.assertEquals(1, provider.getOpenCalls());
        Assertions.assertEquals(1, provider.getActivateCalls());

        RuleDbRuntime.destroy();
        RuleDbRuntime.destroy();

        Assertions.assertEquals(1, provider.getCloseCalls());
    }

    private InMemoryRuleDbProvider provider() {
        return (InMemoryRuleDbProvider) RuleDbProviderHolder.get();
    }

    private ChangeRecord lastChange() {
        return InMemoryRuleRepository.CHANGES.get(InMemoryRuleRepository.CHANGES.size() - 1);
    }
}
