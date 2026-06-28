package com.yomahub.liteflow.test.nodeexecute;

import com.yomahub.liteflow.core.FlowExecutor;
import com.yomahub.liteflow.flow.LiteflowResponse;
import com.yomahub.liteflow.test.BaseTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.TestPropertySource;

import javax.annotation.Resource;

@TestPropertySource(value = "classpath:/nodeexecute/application.properties")
@SpringBootTest(classes = NodeExecuteLifeCycleSpringbootTest.class)
@EnableAutoConfiguration
@ComponentScan({ "com.yomahub.liteflow.test.nodeexecute" })
public class NodeExecuteLifeCycleSpringbootTest extends BaseTest {

    @Resource
    private FlowExecutor flowExecutor;

    @BeforeEach
    public void setUp() {
        NodeExecuteCollector.clear();
    }

    @Test
    public void testSuccessHooks() {
        LiteflowResponse response = flowExecutor.execute2Resp("okChain", "arg");
        Assertions.assertTrue(response.isSuccess());
        // okChain = THEN(neA, neA) → 2 次节点执行
        Assertions.assertEquals(2, NodeExecuteCollector.BEFORE.size());
        Assertions.assertEquals(2, NodeExecuteCollector.afters().size());
        for (NodeExecuteCollector.Record r : NodeExecuteCollector.afters()) {
            Assertions.assertEquals("neA", r.nodeId);
            Assertions.assertNull(r.exception);
            Assertions.assertTrue(r.timeSpent >= 0);
        }
    }

    @Test
    public void testErrorHookCarriesException() {
        LiteflowResponse response = flowExecutor.execute2Resp("boomChain", "arg");
        Assertions.assertFalse(response.isSuccess());
        // boomChain = THEN(neA, neBoom)：neA 成功，neBoom 抛异常
        Assertions.assertEquals(2, NodeExecuteCollector.afters().size());
        NodeExecuteCollector.Record boom = NodeExecuteCollector.afters().get(1);
        Assertions.assertEquals("neBoom", boom.nodeId);
        Assertions.assertNotNull(boom.exception);
        Assertions.assertEquals(IllegalStateException.class, boom.exception.getClass());
    }
}
