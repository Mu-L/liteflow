package com.yomahub.liteflow.publisher;

import com.yomahub.liteflow.publisher.exception.PublisherConfigurationException;
import com.yomahub.liteflow.publisher.exception.PublisherProviderNotFoundException;
import com.yomahub.liteflow.publisher.exception.RuleValidationException;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/** Creates independent publishers from backend-specific typed configurations. */
public final class RulePublisherFactory {

	private RulePublisherFactory() {
	}

	public static RulePublisher create(RulePublisherConfig config) {
		return create(config, ServiceLoader.load(RulePublisherProvider.class));
	}

	static RulePublisher create(RulePublisherConfig config, Iterable<RulePublisherProvider> providers) {
		validateConfig(config);
		List<RulePublisherProvider> matches = new ArrayList<>();
		for (RulePublisherProvider provider : providers) {
			if (provider == null) {
				continue;
			}
			try {
				if (provider.supports(config)) {
					matches.add(provider);
				}
			}
			catch (RuntimeException e) {
				throw new PublisherConfigurationException("publisher provider["
						+ provider.getClass().getName() + "] failed while inspecting config", e);
			}
		}

		if (matches.isEmpty()) {
			throw new PublisherProviderNotFoundException("no RulePublisherProvider supports backend["
					+ config.backend() + "] and config[" + config.getClass().getName() + "]");
		}
		if (matches.size() > 1) {
			List<String> names = new ArrayList<>();
			for (RulePublisherProvider provider : matches) {
				names.add(provider.getClass().getName());
			}
			throw new PublisherConfigurationException("multiple RulePublisherProvider implementations support config["
					+ config.getClass().getName() + "]: " + String.join(", ", names));
		}

		RulePublisher publisher = matches.get(0).create(config);
		if (publisher == null) {
			throw new PublisherConfigurationException("publisher provider["
					+ matches.get(0).getClass().getName() + "] returned null");
		}
		return new ValidatingRulePublisher(publisher);
	}

	private static void validateConfig(RulePublisherConfig config) {
		if (config == null) {
			throw new PublisherConfigurationException("publisher config must not be null");
		}
		if (config.applicationName() == null || config.applicationName().trim().isEmpty()) {
			throw new PublisherConfigurationException("publisher applicationName must not be blank");
		}
		if (config.backend() == null) {
			throw new PublisherConfigurationException("publisher backend must not be null");
		}
	}

	private static final class ValidatingRulePublisher implements RulePublisher {

		private final RulePublisher delegate;

		private ValidatingRulePublisher(RulePublisher delegate) {
			this.delegate = delegate;
		}

		@Override
		public PublishResult publishChain(PublishChainRequest request) {
			requireRequest(request, "publish chain");
			request.validate();
			return delegate.publishChain(request);
		}

		@Override
		public PublishResult publishScript(PublishScriptRequest request) {
			requireRequest(request, "publish script");
			request.validate();
			return delegate.publishScript(request);
		}

		@Override
		public PublishResult removeChain(RemoveRuleRequest request) {
			requireRequest(request, "remove chain");
			request.validate();
			return delegate.removeChain(request);
		}

		@Override
		public PublishResult removeScript(RemoveRuleRequest request) {
			requireRequest(request, "remove script");
			request.validate();
			return delegate.removeScript(request);
		}

		@Override
		public void close() {
			delegate.close();
		}

		private void requireRequest(Object request, String operation) {
			if (request == null) {
				throw new RuleValidationException(operation + " request must not be null");
			}
		}
	}
}
