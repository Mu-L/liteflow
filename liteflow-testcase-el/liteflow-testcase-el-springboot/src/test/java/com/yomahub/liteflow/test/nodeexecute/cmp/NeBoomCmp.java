package com.yomahub.liteflow.test.nodeexecute.cmp;

import com.yomahub.liteflow.core.NodeComponent;
import org.springframework.stereotype.Component;

@Component("neBoom")
public class NeBoomCmp extends NodeComponent {
    @Override
    public void process() {
        throw new IllegalStateException("boom");
    }
}
