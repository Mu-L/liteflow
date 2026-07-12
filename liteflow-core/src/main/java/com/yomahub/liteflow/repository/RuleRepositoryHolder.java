package com.yomahub.liteflow.repository;

import com.yomahub.liteflow.exception.ConfigErrorException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.ServiceLoader;

/**
 * Rule-DB 模式 SPI 解析持有器，基于 {@link ServiceLoader} 懒加载 classpath 上唯一的
 * {@link RuleRepository} 实现，结果缓存至 {@link #reset()} 被调用。
 *
 * @author Bryan.Zhang
 * @since 2.16.1
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
			RuleDbProvider provider = RuleDbProviderHolder.get();
			repository = resolve(impls, provider);
			resolved = true;
		}
		return repository;
	}

	static RuleRepository resolve(Iterable<RuleRepository> legacyCandidates, RuleDbProvider provider) {
		List<RuleRepository> legacy = new ArrayList<>();
		for (RuleRepository candidate : legacyCandidates) {
			if (candidate != null) {
				legacy.add(candidate);
			}
		}
		if (legacy.size() > 1) {
			closeLegacy(legacy);
			closeProvider(provider);
			RuleDbProviderHolder.clearIf(provider);
			throw new ConfigErrorException("multiple RuleRepository implementations found on classpath, keep only one of liteflow-rule-db-sql / liteflow-rule-db-redis");
		}
		if (!legacy.isEmpty() && provider != null) {
			closeLegacy(legacy);
			closeProvider(provider);
			RuleDbProviderHolder.clearIf(provider);
			throw new ConfigErrorException("both legacy RuleRepository and RuleDbProvider are configured; keep only one rule-db integration");
		}
		return legacy.isEmpty() ? (provider == null ? null : provider.repository()) : legacy.get(0);
	}

	private static void closeLegacy(List<RuleRepository> repositories) {
		Set<RuleRepository> closed = Collections.newSetFromMap(new IdentityHashMap<>());
		for (RuleRepository candidate : repositories) {
			if (closed.add(candidate)) {
				try {
					candidate.close();
				} catch (Exception ignored) {
				}
			}
		}
	}

	private static void closeProvider(RuleDbProvider provider) {
		if (provider == null) {
			return;
		}
		try {
			provider.close();
		} catch (Exception ignored) {
		}
	}

	public static boolean hasImplementation() {
		return get() != null;
	}

	/** 供测试与 destroy 重置 ServiceLoader 解析结果 */
	public static synchronized void reset() {
		repository = null;
		resolved = false;
		RuleDbProviderHolder.reset();
	}

	/** Clears only the cached legacy/provider fallback without closing a live provider. */
	static synchronized void clearCached() {
		repository = null;
		resolved = false;
	}

}
