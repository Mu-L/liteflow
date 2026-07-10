package com.yomahub.liteflow.repository.vo;

/**
 * Rule-DB 模式下 chain 的清单元数据（不含 EL 内容），用于 {@link RuleManifest}。
 *
 * @author Bryan.Zhang
 * @since 2.16.2
 */
public class ChainMeta {

	private String chainId;

	private long version;

	private String md5;

	public ChainMeta() {
	}

	public ChainMeta(String chainId, long version, String md5) {
		this.chainId = chainId;
		this.version = version;
		this.md5 = md5;
	}

	public String getChainId() {
		return chainId;
	}

	public void setChainId(String chainId) {
		this.chainId = chainId;
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

}
