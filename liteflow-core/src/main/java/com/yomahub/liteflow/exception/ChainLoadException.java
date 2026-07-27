package com.yomahub.liteflow.exception;

/**
 * Rule-DB 模式下回源加载失败（区别于 ChainNotFoundException：规则存在但取不回来）
 *
 * @author Bryan.Zhang
 * @since 2.16.1
 */
public class ChainLoadException extends LiteFlowException {

	private static final long serialVersionUID = 1L;

	public ChainLoadException(String message) {
		super(message);
	}

}
