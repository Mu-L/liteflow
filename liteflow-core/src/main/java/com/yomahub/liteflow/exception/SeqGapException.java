package com.yomahub.liteflow.exception;

/**
 * Rule-DB 模式增量拉取时检测到 seq 已断档（变更日志被清理），需要全量对账补偿
 *
 * @author Bryan.Zhang
 * @since 2.16.2
 */
public class SeqGapException extends LiteFlowException {

	private static final long serialVersionUID = 1L;

	public SeqGapException(String message) {
		super(message);
	}

}
