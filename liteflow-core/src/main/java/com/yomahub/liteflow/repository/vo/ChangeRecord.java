package com.yomahub.liteflow.repository.vo;

/**
 * Rule-DB 模式变更日志中的一条增量记录，由 {@code RuleRepository.fetchChangesSince(seq)} 返回。
 *
 * @author Bryan.Zhang
 * @since 2.16.2
 */
public class ChangeRecord {

	public enum TargetType {
		CHAIN, SCRIPT
	}

	public enum Op {
		UPSERT, DELETE
	}

	private long seq;

	private TargetType targetType;

	private String targetId;

	private Op op;

	private long version;

	public ChangeRecord() {
	}

	public ChangeRecord(long seq, TargetType targetType, String targetId, Op op, long version) {
		this.seq = seq;
		this.targetType = targetType;
		this.targetId = targetId;
		this.op = op;
		this.version = version;
	}

	public long getSeq() {
		return seq;
	}

	public void setSeq(long seq) {
		this.seq = seq;
	}

	public TargetType getTargetType() {
		return targetType;
	}

	public void setTargetType(TargetType targetType) {
		this.targetType = targetType;
	}

	public String getTargetId() {
		return targetId;
	}

	public void setTargetId(String targetId) {
		this.targetId = targetId;
	}

	public Op getOp() {
		return op;
	}

	public void setOp(Op op) {
		this.op = op;
	}

	public long getVersion() {
		return version;
	}

	public void setVersion(long version) {
		this.version = version;
	}

}
