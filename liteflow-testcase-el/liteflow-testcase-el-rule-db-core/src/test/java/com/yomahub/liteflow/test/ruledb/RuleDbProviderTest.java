package com.yomahub.liteflow.test.ruledb;

import com.yomahub.liteflow.repository.ChangeSourceHealth;
import com.yomahub.liteflow.repository.RuleDbProvider;
import com.yomahub.liteflow.repository.RuleDbProviderHolder;
import com.yomahub.liteflow.repository.RuleRepository;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RuleDbProviderTest {

    @AfterEach
    void reset() {
        RuleDbProviderHolder.reset();
        InMemoryRuleRepository.reset();
    }

    @Test
    void resolvesProviderAndSharesRepository() {
        RuleDbProvider provider = RuleDbProviderHolder.get();

        assertSame(provider, RuleDbProviderHolder.get());
        assertSame(provider.repository(), RuleDbProviderHolder.repository());
    }

    @Test
    void healthStartsInStartingState() {
        assertEquals(ChangeSourceHealth.Status.STARTING,
                RuleDbProviderHolder.get().changeSource().health().getStatus());
    }

    @Test
    void buffersUntilActivationAndDropsBaselineEvents() {
        RuleDbProvider provider = RuleDbProviderHolder.get();
        List<ChangeRecord> delivered = new ArrayList<>();
        provider.changeSource().open(delivered::addAll);
        ((InMemoryRuleDbProvider) provider).emit(new ChangeRecord(1, ChangeRecord.TargetType.CHAIN,
                "old", ChangeRecord.Op.UPSERT, 1));
        ((InMemoryRuleDbProvider) provider).emit(new ChangeRecord(3, ChangeRecord.TargetType.CHAIN,
                "new", ChangeRecord.Op.UPSERT, 1));

        provider.changeSource().activate(1);

        assertEquals(1, delivered.size());
        assertEquals(3, delivered.get(0).getSeq());
        assertEquals(ChangeSourceHealth.Status.UP,
                provider.changeSource().health().getStatus());
    }

    @Test
    void closeMakesLateEventsNoOp() {
        RuleDbProvider provider = RuleDbProviderHolder.get();
        List<ChangeRecord> delivered = new ArrayList<>();
        provider.changeSource().open(delivered::addAll);
        provider.changeSource().activate(0);
        provider.changeSource().close();
        ((InMemoryRuleDbProvider) provider).emit(new ChangeRecord(1, ChangeRecord.TargetType.CHAIN,
                "late", ChangeRecord.Op.UPSERT, 1));
        provider.close();

        assertTrue(delivered.isEmpty());
        assertEquals(ChangeSourceHealth.Status.DOWN,
                provider.changeSource().health().getStatus());
    }
}
