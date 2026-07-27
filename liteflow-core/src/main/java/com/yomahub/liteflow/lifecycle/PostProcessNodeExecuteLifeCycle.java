package com.yomahub.liteflow.lifecycle;

import com.yomahub.liteflow.core.NodeComponent;

/**
 * 生命周期接口
 * 执行单个组件（Node）的时候
 *
 * @author Bryan.Zhang
 */
public interface PostProcessNodeExecuteLifeCycle extends LifeCycle {

    void postProcessBeforeNodeExecute(NodeComponent cmp);

    /**
     * @param cmp       执行完成的组件（含 nodeId / chainId / type）
     * @param timeSpent 本次执行耗时（毫秒）
     * @param e         执行异常，成功时为 null
     */
    void postProcessAfterNodeExecute(NodeComponent cmp, long timeSpent, Exception e);
}
