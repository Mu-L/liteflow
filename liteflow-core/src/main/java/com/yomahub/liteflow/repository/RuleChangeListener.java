package com.yomahub.liteflow.repository;

import com.yomahub.liteflow.repository.vo.ChangeRecord;

import java.util.List;

/**
 * Rule-DB 模式推送通道监听器，由 {@link RuleRepository#subscribe(RuleChangeListener)} 注册。
 * Redis 等支持发布订阅的实现可在变更发生时回调，避免轮询。
 *
 * @author Bryan.Zhang
 * @since 2.16.1
 */
public interface RuleChangeListener {

	void onChanges(List<ChangeRecord> changes);

}
