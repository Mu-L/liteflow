package com.yomahub.liteflow.metrics;

import com.yomahub.liteflow.flow.FlowBus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class LiteflowMetaViewTest {

    @Test
    public void testOverviewAndChains() {
        FlowBus.addChain("viewChain");

        LiteflowMetaView view = new LiteflowMetaView(null);

        Map<String, Object> overview = view.overview();
        Assertions.assertTrue(((Number) overview.get("chainsRegistered")).intValue() >= 1);
        Assertions.assertTrue(overview.containsKey("nodesRegistered"));

        List<Map<String, Object>> chains = view.chains();
        boolean found = chains.stream().anyMatch(c -> "viewChain".equals(c.get("chainId")));
        Assertions.assertTrue(found);
    }

    @Test
    public void testUnknownChainReturnsError() {
        LiteflowMetaView view = new LiteflowMetaView(null);
        Map<String, Object> result = view.chain("__not_exist__");
        Assertions.assertTrue(result.containsKey("error"));
    }
}
