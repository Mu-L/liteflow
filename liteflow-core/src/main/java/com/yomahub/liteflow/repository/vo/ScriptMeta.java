package com.yomahub.liteflow.repository.vo;

/**
 * Rule-DB 模式下 script 的清单元数据（不含脚本内容），用于 {@link RuleManifest}。
 *
 * @author Bryan.Zhang
 * @since 2.16.1
 */
public class ScriptMeta {

	private String nodeId;

	private long version;

	private String md5;

	// NodeTypeEnum code：script/boolean_script/switch_script/...
	private String type;

	// 可空
	private String language;

	// 可空
	private String name;

	public ScriptMeta() {
	}

	public ScriptMeta(String nodeId, long version, String md5, String type, String language, String name) {
		this.nodeId = nodeId;
		this.version = version;
		this.md5 = md5;
		this.type = type;
		this.language = language;
		this.name = name;
	}

	public String getNodeId() {
		return nodeId;
	}

	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
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

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getLanguage() {
		return language;
	}

	public void setLanguage(String language) {
		this.language = language;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

}
