package com.yomahub.liteflow.test.ruledb;

import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.repository.ChangeSourceHealth;
import com.yomahub.liteflow.repository.RuleDbProviderHolder;
import com.yomahub.liteflow.repository.RuleDbRuntime;
import com.yomahub.liteflow.repository.runtime.RuleTargetState;
import com.yomahub.liteflow.repository.runtime.RuleTargetStatus;
import com.yomahub.liteflow.repository.vo.RuleDbRuntimeSnapshot;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RuleDbRuntimeSnapshotTest extends BaseRuleDbTest {

    @Test
    void reportsContentFreeRuntimeHealthAndBoundedFailures() {
        for (int i = 0; i < 25; i++) {
            InMemoryRuleRepository.publishChain(targetId(i), "THEN(a)");
        }
        buildExecutor(new RuleDbConfig());
        InMemoryRuleDbProvider provider = (InMemoryRuleDbProvider) RuleDbProviderHolder.get();

        for (int i = 0; i < 25; i++) {
            RuleTargetState state = RuleDbRuntime.chainState(targetId(i));
            state.markFailed(new IllegalStateException("compile failed"));
        }
        provider.requestReconcile();

        RuleDbRuntimeSnapshot snapshot = RuleDbRuntime.snapshot();
        Assertions.assertTrue(snapshot.isActive());
        Assertions.assertEquals("memory", snapshot.getProvider());
        Assertions.assertEquals(ChangeSourceHealth.Status.UP, snapshot.getChangeSource().getStatus());
        Assertions.assertEquals(25, snapshot.getTargetCounts().get(RuleTargetStatus.FAILED));
        Assertions.assertTrue(snapshot.getLastSuccessfulReconcileTime() > 0);
        Assertions.assertEquals(20, snapshot.getFailedTargets().size());
        RuleDbRuntimeSnapshot.FailedTarget failure = snapshot.getFailedTargets().get(0);
        Assertions.assertEquals("snapshotChain00", failure.getTargetId());
        Assertions.assertEquals(1L, failure.getDesiredVersion());
        Assertions.assertEquals("compile failed", failure.getError());
    }

    private String targetId(int index) {
        return "snapshotChain" + (index < 10 ? "0" : "") + index;
    }
}
