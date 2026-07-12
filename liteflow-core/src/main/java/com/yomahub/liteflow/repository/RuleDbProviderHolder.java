package com.yomahub.liteflow.repository;

import com.yomahub.liteflow.exception.ConfigErrorException;

import java.util.ArrayList;
import java.util.List;
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
			if (providers.size() > 1) {
				List<String> names = new ArrayList<>();
				for (RuleDbProvider candidate : providers) {
					names.add(candidate.getClass().getName());
				}
				throw new ConfigErrorException("multiple RuleDbProvider implementations found: "
						+ String.join(", ", names));
			}
			provider = providers.isEmpty() ? null : providers.get(0);
			resolved = true;
		}
		return provider;
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
}
