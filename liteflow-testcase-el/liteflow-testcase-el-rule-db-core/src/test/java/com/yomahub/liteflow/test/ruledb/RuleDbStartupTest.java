package com.yomahub.liteflow.test.ruledb;

import com.yomahub.liteflow.enums.NodeTypeEnum;
import com.yomahub.liteflow.flow.FlowBus;
import com.yomahub.liteflow.flow.element.Chain;
import com.yomahub.liteflow.flow.element.Node;
import com.yomahub.liteflow.property.RuleDbConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RuleDbStartupTest extends BaseRuleDbTest {

    @Test
    public void testStartupBuildsShadowIndexWithoutFetchingContent() {
        InMemoryRuleRepository.putChain("chain1", "THEN(a, b)");
        InMemoryRuleRepository.putScript("s1", "println('s1 run')", "script", "groovy");
        registerCommonCmp();

        buildExecutor(new RuleDbConfig());

        // chain 影子已注册但未编译、无 EL
        Chain chain = FlowBus.getChain("chain1");
        Assertions.assertNotNull(chain);
        Assertions.assertFalse(chain.isCompiled());
        Assertions.assertNull(chain.getEl());

        // 脚本影子已注册：有元数据、无源码
        Node node = FlowBus.getNode("s1");
        Assertions.assertNotNull(node);
        Assertions.assertEquals(NodeTypeEnum.SCRIPT, node.getType());
        Assertions.assertEquals("groovy", node.getLanguage());
        Assertions.assertNull(node.getScript());

        // 未发生任何内容回源
        Assertions.assertEquals(0, InMemoryRuleRepository.FETCH_CHAIN_COUNT.get());
        Assertions.assertEquals(0, InMemoryRuleRepository.FETCH_SCRIPT_COUNT.get());
    }
}
