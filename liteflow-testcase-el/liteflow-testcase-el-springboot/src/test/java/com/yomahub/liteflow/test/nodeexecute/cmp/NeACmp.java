package com.yomahub.liteflow.test.nodeexecute.cmp;

import com.yomahub.liteflow.core.NodeComponent;
import org.springframework.stereotype.Component;

@Component("neA")
public class NeACmp extends NodeComponent {
    @Override
    public void process() {
        // 正常组件
    }
}
