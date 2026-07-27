package com.yomahub.liteflow.property;

/** Rule-DB executable cache configuration. */
public class RuleDbCacheConfig {

	private Integer capacity = 500;

	private String preloadChainIds;

	public Integer getCapacity() {
		return capacity;
	}

	public void setCapacity(Integer capacity) {
		this.capacity = capacity;
	}

	public String getPreloadChainIds() {
		return preloadChainIds;
	}

	public void setPreloadChainIds(String preloadChainIds) {
		this.preloadChainIds = preloadChainIds;
	}
}
