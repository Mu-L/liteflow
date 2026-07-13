package com.yomahub.liteflow.property;

/** Rule-DB change detection and reconciliation configuration. */
public class RuleDbSyncConfig {

	private Integer pollSeconds;

	private Integer reconcileSeconds = 60;

	private Integer fetchRetryTimes = 3;

	public Integer getPollSeconds() {
		return pollSeconds;
	}

	public void setPollSeconds(Integer pollSeconds) {
		this.pollSeconds = pollSeconds;
	}

	public Integer getReconcileSeconds() {
		return reconcileSeconds;
	}

	public void setReconcileSeconds(Integer reconcileSeconds) {
		this.reconcileSeconds = reconcileSeconds;
	}

	public Integer getFetchRetryTimes() {
		return fetchRetryTimes;
	}

	public void setFetchRetryTimes(Integer fetchRetryTimes) {
		this.fetchRetryTimes = fetchRetryTimes;
	}
}
