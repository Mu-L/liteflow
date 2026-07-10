package com.yomahub.liteflow.repository;

import com.yomahub.liteflow.exception.ConfigErrorException;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/**
 * Rule-DB 模式 SPI 解析持有器，基于 {@link ServiceLoader} 懒加载 classpath 上唯一的
 * {@link RuleRepository} 实现，结果缓存至 {@link #reset()} 被调用。
 *
 * @author Bryan.Zhang
 * @since 2.16.2
 */
public class RuleRepositoryHolder {

	private static volatile RuleRepository repository;

	private static volatile boolean resolved = false;

	public static synchronized RuleRepository get() {
		if (!resolved) {
			List<RuleRepository> impls = new ArrayList<>();
			for (RuleRepository r : ServiceLoader.load(RuleRepository.class)) {
				impls.add(r);
			}
			if (impls.size() > 1) {
				throw new ConfigErrorException(
						"multiple RuleRepository implementations found on classpath, keep only one of liteflow-rule-db-sql / liteflow-rule-db-redis");
			}
			repository = impls.isEmpty() ? null : impls.get(0);
			resolved = true;
		}
		return repository;
	}

	public static boolean hasImplementation() {
		return get() != null;
	}

	/** 供测试与 destroy 重置 ServiceLoader 解析结果 */
	public static synchronized void reset() {
		repository = null;
		resolved = false;
	}

}
