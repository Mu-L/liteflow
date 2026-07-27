package com.yomahub.liteflow.metrics;

import com.yomahub.liteflow.flow.FlowBus;
import com.yomahub.liteflow.flow.element.Chain;
import com.yomahub.liteflow.flow.element.Node;
import com.yomahub.liteflow.meta.LiteflowMetaOperator;
import com.yomahub.liteflow.repository.ChangeSourceHealth;
import com.yomahub.liteflow.repository.RuleDbRuntime;
import com.yomahub.liteflow.repository.runtime.RuleTargetStatus;
import com.yomahub.liteflow.repository.vo.RuleDbRuntimeSnapshot;
import com.yomahub.liteflow.slot.DataBus;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.search.Search;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 结构检视 + 指标快照取数（纯 POJO，供 actuator 端点委托）
 *
 * @author Bryan.Zhang
 */
public class LiteflowMetaView {

    /** 可空：无 Micrometer 时仅返回结构信息 */
    private final MeterRegistry registry;

    public LiteflowMetaView(MeterRegistry registry) {
        this.registry = registry;
    }

    public Map<String, Object> overview() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("chainsRegistered", FlowBus.getChainMap().size());
        m.put("nodesRegistered", FlowBus.getNodeMap().size());
        m.put("slotOccupied", DataBus.OCCUPY_COUNT.get());
        m.put("chainIds", new ArrayList<>(FlowBus.getChainMap().keySet()));
        m.put("nodeIds", new ArrayList<>(FlowBus.getNodeMap().keySet()));
        return m;
    }

    public List<Map<String, Object>> chains() {
        List<Map<String, Object>> list = new ArrayList<>();
        for (Chain chain : FlowBus.getChainMap().values()) {
            list.add(chainBrief(chain));
        }
        return list;
    }

    public Map<String, Object> chain(String chainId) {
        Chain chain = FlowBus.getChainMap().get(chainId);
        if (chain == null) {
            return error("chain not found: " + chainId);
        }
        Map<String, Object> m = chainBrief(chain);
        m.put("metrics", chainMetrics(chainId));
        return m;
    }

    public List<Map<String, Object>> nodes() {
        List<Map<String, Object>> list = new ArrayList<>();
        for (Node node : FlowBus.getNodeMap().values()) {
            list.add(nodeBrief(node));
        }
        return list;
    }

    public Map<String, Object> node(String nodeId) {
        Node node = FlowBus.getNodeMap().get(nodeId);
        if (node == null) {
            return error("node not found: " + nodeId);
        }
        Map<String, Object> m = nodeBrief(node);
        m.put("metrics", nodeMetrics(nodeId));
        List<String> inChains = new ArrayList<>();
        for (Chain c : LiteflowMetaOperator.getChainsContainsNodeId(nodeId)) {
            inChains.add(c.getChainId());
        }
        m.put("inChains", inChains);
        return m;
    }

    public Map<String, Object> ruleDb() {
        RuleDbRuntimeSnapshot snapshot = RuleDbRuntime.snapshot();
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("active", snapshot.isActive());
        if (!snapshot.isActive()) {
            return result;
        }
        result.put("provider", snapshot.getProvider());

        ChangeSourceHealth source = snapshot.getChangeSource();
        Map<String, Object> sourceView = new LinkedHashMap<>();
        sourceView.put("status", source == null ? null : source.getStatus().name());
        sourceView.put("recentError", source == null ? null : source.getRecentError());
        sourceView.put("lastSuccessTime", source == null ? 0L : source.getLastSuccessTime());
        sourceView.put("cursor", source == null ? 0L : source.getCursor());
        result.put("changeSource", sourceView);

        result.put("lastAppliedSeq", snapshot.getLastAppliedSeq());
        result.put("lastSuccessfulReconcileTime", snapshot.getLastSuccessfulReconcileTime());
        result.put("lastReconcileError", snapshot.getLastReconcileError());

        Map<String, Object> targets = new LinkedHashMap<>();
        for (RuleTargetStatus status : RuleTargetStatus.values()) {
            targets.put(status.name().toLowerCase(Locale.ROOT), snapshot.getTargetCounts().get(status));
        }
        result.put("targets", targets);

        List<Map<String, Object>> failures = new ArrayList<>();
        for (RuleDbRuntimeSnapshot.FailedTarget target : snapshot.getFailedTargets()) {
            Map<String, Object> failure = new LinkedHashMap<>();
            failure.put("targetType", target.getTargetType().name().toLowerCase(Locale.ROOT));
            failure.put("targetId", target.getTargetId());
            failure.put("status", target.getStatus().name());
            failure.put("desiredVersion", target.getDesiredVersion());
            failure.put("activeVersion", target.getActiveVersion());
            failure.put("error", target.getError());
            failures.add(failure);
        }
        result.put("failedTargets", failures);
        return result;
    }

    private Map<String, Object> chainBrief(Chain chain) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("chainId", chain.getChainId());
        m.put("namespace", chain.getNamespace());
        m.put("el", chain.getEl());
        m.put("elMd5", chain.getElMd5());
        return m;
    }

    private Map<String, Object> nodeBrief(Node node) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("nodeId", node.getId());
        m.put("name", node.getName());
        m.put("type", node.getType() == null ? null : node.getType().name());
        m.put("script", node.getType() != null && node.getType().isScript());
        m.put("clazz", node.getClazz());
        m.put("language", node.getLanguage());
        return m;
    }

    private Map<String, Object> chainMetrics(String chainId) {
        if (registry == null) {
            return null;
        }
        return timerSnapshot(Search.in(registry).name("liteflow.chain.executions").tag("chain", chainId).timers());
    }

    private Map<String, Object> nodeMetrics(String nodeId) {
        if (registry == null) {
            return null;
        }
        return timerSnapshot(Search.in(registry).name("liteflow.node.executions").tag("node", nodeId).timers());
    }

    /** 把同名（不同 status）的多个 Timer 汇总成一个快照 */
    private Map<String, Object> timerSnapshot(java.util.Collection<Timer> timers) {
        long count = 0;
        long failed = 0;
        double totalMs = 0;
        double maxMs = 0;
        for (Timer t : timers) {
            long c = t.count();
            count += c;
            totalMs += t.totalTime(TimeUnit.MILLISECONDS);
            maxMs = Math.max(maxMs, t.max(TimeUnit.MILLISECONDS));
            if ("failed".equals(t.getId().getTag("status"))) {
                failed += c;
            }
        }
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("count", count);
        m.put("failed", failed);
        m.put("errorRate", count == 0 ? 0.0 : (double) failed / count);
        m.put("meanMs", count == 0 ? 0.0 : totalMs / count);
        m.put("maxMs", maxMs);
        return m;
    }

    public Map<String, Object> error(String msg) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("error", msg);
        return m;
    }
}
