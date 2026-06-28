package com.yomahub.liteflow.springboot.metrics;

import com.yomahub.liteflow.metrics.LiteflowMetaView;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.Selector;

import java.util.List;
import java.util.Map;

/**
 * LiteFlow 结构检视 actuator 端点：/actuator/liteflow
 *
 * @author Bryan.Zhang
 */
@Endpoint(id = "liteflow")
public class LiteflowEndpoint {

    private final LiteflowMetaView metaView;

    public LiteflowEndpoint(LiteflowMetaView metaView) {
        this.metaView = metaView;
    }

    @ReadOperation
    public Map<String, Object> overview() {
        return metaView.overview();
    }

    @ReadOperation
    public Object chains(@Selector String level) {
        // /actuator/liteflow/chains 或 /actuator/liteflow/nodes
        if ("chains".equals(level)) {
            return metaView.chains();
        }
        if ("nodes".equals(level)) {
            return metaView.nodes();
        }
        return metaView.error("unknown selector: " + level);
    }

    @ReadOperation
    public Map<String, Object> detail(@Selector String level, @Selector String id) {
        // /actuator/liteflow/chains/{id} 或 /actuator/liteflow/nodes/{id}
        if ("chains".equals(level)) {
            return metaView.chain(id);
        }
        if ("nodes".equals(level)) {
            return metaView.node(id);
        }
        return metaView.error("unknown selector: " + level);
    }
}
