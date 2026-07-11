package com.yomahub.liteflow.test.metrics.cmp;

import com.yomahub.liteflow.core.NodeComponent;
import org.springframework.stereotype.Component;

@Component("f")
public class MfCmp extends NodeComponent {
    @Override
    public void process() {
        throw new IllegalStateException("metrics test error in catch");
    }
}
