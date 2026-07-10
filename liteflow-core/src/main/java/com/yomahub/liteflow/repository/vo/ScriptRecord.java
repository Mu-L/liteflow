package com.yomahub.liteflow.repository.vo;

/**
 * Rule-DB 模式下按 nodeId 回源取到的脚本完整记录（含脚本内容）。
 *
 * @author Bryan.Zhang
 * @since 2.16.2
 */
public class ScriptRecord {

	private String nodeId;

	private String script;

	// 可空
	private String name;

	// NodeTypeEnum code
	private String type;

	// 可空
	private String language;

	private long version;

	private String md5;

	private boolean enable = true;

	public String getNodeId() {
		return nodeId;
	}

	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}

	public String getScript() {
		return script;
	}

	public void setScript(String script) {
		this.script = script;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
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
