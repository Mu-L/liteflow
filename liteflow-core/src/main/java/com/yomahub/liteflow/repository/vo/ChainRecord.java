package com.yomahub.liteflow.repository.vo;

/**
 * Rule-DB 模式下按 id 回源取到的 chain 完整记录（含 EL 内容）。
 *
 * @author Bryan.Zhang
 * @since 2.16.1
 */
public class ChainRecord {

	private String chainId;

	private String el;

	// 可空
	private String route;

	// 可空
	private String namespace;

	private long version;

	private String md5;

	private boolean enable = true;

	public String getChainId() {
		return chainId;
	}

	public void setChainId(String chainId) {
		this.chainId = chainId;
	}

	public String getEl() {
		return el;
	}

	public void setEl(String el) {
		this.el = el;
	}

	public String getRoute() {
		return route;
	}

	public void setRoute(String route) {
		this.route = route;
	}

	public String getNamespace() {
		return namespace;
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	public long getVersion() {
		return version;
	}

	public void setVersion(long version) {
		this.version = version;
	}

	public String getMd5() {
		return md5;
	}

	public void setMd5(String md5) {
		this.md5 = md5;
	}

	public boolean isEnable() {
		return enable;
	}

	public void setEnable(boolean enable) {
		this.enable = enable;
	}

}
