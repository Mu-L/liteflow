package com.yomahub.liteflow.repository;

import com.yomahub.liteflow.exception.ConfigErrorException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.ServiceLoader;

/** Lazy, classpath-wide resolver for the single Rule-DB provider. */
public final class RuleDbProviderHolder {

	private static volatile RuleDbProvider provider;
	private static volatile boolean resolved;

	private RuleDbProviderHolder() {
	}

	public static synchronized RuleDbProvider get() {
		if (!resolved) {
			List<RuleDbProvider> providers = new ArrayList<>();
			for (RuleDbProvider candidate : ServiceLoader.load(RuleDbProvider.class)) {
				providers.add(candidate);
			}
			provider = resolve(providers);
			resolved = true;
		}
		return provider;
	}

	/**
	 * Resolves the provider set. Package-private so the core contract tests can
	 * exercise resolution without replacing the application class loader.
	 */
	static RuleDbProvider resolve(Iterable<RuleDbProvider> candidates) {
		RuleDbProvider resolvedProvider = null;
		List<String> names = new ArrayList<>();
		List<RuleDbProvider> instantiated = new ArrayList<>();
		for (RuleDbProvider candidate : candidates) {
			if (candidate == null) {
				continue;
			}
			names.add(candidate.getClass().getName());
			instantiated.add(candidate);
			if (resolvedProvider == null) {
				resolvedProvider = candidate;
			}
		}
		if (names.size() > 1) {
			Set<RuleDbProvider> closed = Collections.newSetFromMap(new IdentityHashMap<>());
			for (RuleDbProvider candidate : instantiated) {
				if (candidate != null && closed.add(candidate)) {
					try {
						candidate.close();
					} catch (Exception ignored) {
						// Preserve the resolution error while closing every candidate best effort.
					}
				}
			}
			throw new ConfigErrorException("multiple RuleDbProvider implementations found: "
					+ String.join(", ", names));
		}
		return resolvedProvider;
	}

	public static RuleRepository repository() {
		RuleDbProvider current = get();
		return current == null ? null : current.repository();
	}

	public static boolean hasImplementation() {
		return get() != null;
	}

	/** Reset cached resolution and close the provider currently held by the resolver. */
	public static synchronized void reset() {
		RuleDbProvider current = provider;
		provider = null;
		resolved = false;
		if (current != null) {
			try {
				current.close();
			} catch (Exception ignored) {
				// Reset is best effort and must remain usable in test cleanup.
			}
		}
	}

	/** Clears a provider whose lifecycle was already closed by the runtime. */
	static synchronized void clearIf(RuleDbProvider expected) {
		if (provider == expected) {
			provider = null;
			resolved = false;
		}
	}
}
