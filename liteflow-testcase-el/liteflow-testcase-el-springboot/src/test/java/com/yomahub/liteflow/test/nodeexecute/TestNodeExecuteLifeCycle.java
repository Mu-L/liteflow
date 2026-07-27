package com.yomahub.liteflow.test.nodeexecute;

import com.yomahub.liteflow.core.NodeComponent;
import com.yomahub.liteflow.lifecycle.PostProcessNodeExecuteLifeCycle;
import org.springframework.stereotype.Component;

@Component
public class TestNodeExecuteLifeCycle implements PostProcessNodeExecuteLifeCycle {
    @Override
    public void postProcessBeforeNodeExecute(NodeComponent cmp) {
        NodeExecuteCollector.BEFORE.add(cmp.getNodeId());
    }
    @Override
    public void postProcessAfterNodeExecute(NodeComponent cmp, long timeSpent, Exception e) {
        NodeExecuteCollector.AFTER.add(new NodeExecuteCollector.Record(cmp.getNodeId(), timeSpent, e));
    }
}
