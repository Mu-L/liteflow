package com.yomahub.liteflow.builder.el.operator;

import com.yomahub.liteflow.builder.el.operator.base.BaseOperator;
import com.yomahub.liteflow.builder.el.operator.base.OperatorHelper;
import com.yomahub.liteflow.flow.element.Chain;
import com.yomahub.liteflow.flow.element.Condition;
import com.yomahub.liteflow.flow.element.Executable;
import com.yomahub.liteflow.flow.element.Node;
import com.yomahub.liteflow.flow.element.condition.ChainBindWrapperCondition;
import com.yomahub.liteflow.meta.LiteflowMetaOperator;

/**
 * EL规则中的bind的操作符
 *
 * @author Bryan.Zhang
 * @since 2.13.0
 */
public class BindOperator extends BaseOperator<Executable> {
    @Override
    public Executable build(Object[] objects) throws Exception {
        OperatorHelper.checkObjectSizeEq(objects, 3, 4);

        Executable bindItem = OperatorHelper.convert(objects[0], Executable.class);

        String key = OperatorHelper.convert(objects[1], String.class);

        String value = OperatorHelper.convert(objects[2], String.class);

        // 获取 override 参数（第四个参数，默认为 false）
        boolean override = false;
        if (objects.length > 3) {
            override = OperatorHelper.convert(objects[3], Boolean.class);
        }

        // 场景1：对 Node bind（保持现有逻辑，bind 数据存在 Node 上）
        if (bindItem instanceof Node) {
            Node node = (Node) bindItem;
            node.putBindData(key, value);
            return node;
        }

        // 场景2：对 Condition bind（如 THEN(...).bind(...)），bind 数据存在 Condition 上
        if (bindItem instanceof Condition) {
            Condition condition = (Condition) bindItem;
            condition.putBindData(key, value);
            // 如果 override=true，需要清除该 Condition 下所有 Node 上相同 key 的 bind 数据
            // 这样可以确保 Condition 级别的 bind 能够覆盖 Node 级别的 bind
            if (override) {
                clearNodeBindData(condition, key);
            }
            // 将 bind 数据下放到该 Condition 直属（不跨子 chain 引用）的节点上，
            // 使流程执行结束后仍能通过 Node#getBindData（如 CmpStep.getRefNode().getBindData(...)）读取到，
            // 与 2.15.1 的表现保持一致。
            // 注意：这里只下放到属于当前 chain 的节点克隆，不会递归进被引用的子 chain，
            // 从而避免污染被多个 chain 共享的子 chain（见 #ID7OTO / #IDCBQ2）。
            putBindDataToLocalNodes(condition, key, value, override);
            return condition;
        }

        // 场景3：对 Chain bind（新逻辑：包装成 ChainBindWrapperCondition）
        // 这样不会修改 Chain 本身，而是创建一个包装 Condition 来持有 bind 数据
        // 从而避免多个 chain 引用同一个子 chain 时的 bind 数据污染问题
        if (bindItem instanceof Chain) {
            Chain chain = (Chain) bindItem;
            ChainBindWrapperCondition wrapper = new ChainBindWrapperCondition(chain);
            wrapper.putBindData(key, value);
            return wrapper;
        }

        return bindItem;
    }

    /**
     * 清除 Condition 下所有 Node 上指定 key 的 bind 数据
     * 用于 override=true 时，确保 Condition 级别的 bind 能够覆盖 Node 级别的 bind
     */
    private void clearNodeBindData(Condition condition, String key) {
        LiteflowMetaOperator.getNodes(condition).forEach(node -> {
            if (node.hasBindData(key)) {
                node.removeBindData(key);
            }
        });
    }

    /**
     * 将 bind 数据下放到该 Condition 直属的节点上（递归进嵌套 Condition，但不跨子 chain 引用）。
     * <p>
     * 这些节点是当前 chain 构建时 clone 出来的、专属于本 chain 的实例（每个 clone 持有独立的
     * bindDataMap），因此写入 bind 数据不会污染被多个 chain 共享的子 chain。
     * 下放之后，流程执行结束仍可通过 {@code CmpStep.getRefNode().getBindData(key)} 读取到表达式级 bind 的值。
     * <p>
     * 对于条件中以 chainId 形式引用的子 chain（{@link Chain} 类型，对象在 FlowBus 中被多个 chain 共享），
     * 这里不会写入其内部节点，其 bind 数据仍由运行时通过 Condition 调用栈解析，从而避免数据污染（见 #ID7OTO / #IDCBQ2）。
     *
     * @param condition 目标 Condition
     * @param key       bind 数据的 key
     * @param value     bind 数据的 value
     * @param override  为 true 时强制覆盖节点级已有的同 key 数据，否则保留节点级 bind（节点级优先）
     */
    private void putBindDataToLocalNodes(Condition condition, String key, String value, boolean override) {
        condition.getExecutableGroup().values().forEach(executableList -> executableList.forEach(executable -> {
            if (executable instanceof Node) {
                Node node = (Node) executable;
                // 节点级 bind 优先：非 override 情况下，节点已有同 key 数据时不覆盖
                if (override || !node.hasBindData(key)) {
                    node.putBindData(key, value);
                }
            } else if (executable instanceof Condition) {
                // 递归进嵌套 Condition（同属当前 chain）；Chain 类型不进入此分支，避免污染共享子 chain
                putBindDataToLocalNodes((Condition) executable, key, value, override);
            }
        }));
    }
}
