package com.yomahub.liteflow.publisher;

import com.yomahub.liteflow.publisher.exception.PublisherConfigurationException;
import com.yomahub.liteflow.publisher.exception.PublisherProviderNotFoundException;
import com.yomahub.liteflow.publisher.exception.RuleStorageException;
import com.yomahub.liteflow.publisher.exception.RuleValidationException;
import com.yomahub.liteflow.repository.vo.ChangeRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

/** Creates independent publishers from backend-specific typed configurations. */
public final class RulePublisherFactory {

	private RulePublisherFactory() {
	}

	/**
	 * Creates a publisher for the supplied configuration by discovering
	 * {@link RulePublisherProvider} implementations through the thread context
	 * {@link ServiceLoader} and selecting the single provider that supports it.
	 *
	 * @param config backend-specific typed configuration, must not be {@code null}
	 * @return a publisher that validates every request before delegating to the backend
	 * @throws PublisherConfigurationException if the configuration is invalid, provider
	 * loading fails, resolution is ambiguous or the selected provider misbehaves
	 * @throws PublisherProviderNotFoundException if no provider supports the configuration
	 */
	public static RulePublisher create(RulePublisherConfig config) {
		List<RulePublisherProvider> providers = new ArrayList<>();
		try {
			for (RulePublisherProvider provider : ServiceLoader.load(RulePublisherProvider.class)) {
				providers.add(provider);
			}
		}
		catch (ServiceConfigurationError e) {
			throw new PublisherConfigurationException(
					"failed to load RulePublisherProvider implementations via ServiceLoader", e);
		}
		return create(config, providers);
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

		RulePublisherProvider provider = matches.get(0);
		RulePublisher publisher;
		try {
			publisher = provider.create(config);
		}
		catch (PublisherConfigurationException e) {
			throw e;
		}
		catch (RuntimeException e) {
			throw new PublisherConfigurationException("publisher provider["
					+ provider.getClass().getName() + "] failed while creating publisher", e);
		}
		if (publisher == null) {
			throw new PublisherConfigurationException("publisher provider["
					+ provider.getClass().getName() + "] returned null");
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
			return requireResult(delegate.publishChain(request), request.getChainId(),
					ChangeRecord.TargetType.CHAIN, ChangeRecord.Op.UPSERT, "publish chain");
		}

		@Override
		public PublishResult publishScript(PublishScriptRequest request) {
			requireRequest(request, "publish script");
			request.validate();
			return requireResult(delegate.publishScript(request), request.getNodeId(),
					ChangeRecord.TargetType.SCRIPT, ChangeRecord.Op.UPSERT, "publish script");
		}

		@Override
		public PublishResult removeChain(RemoveRuleRequest request) {
			requireRequest(request, "remove chain");
			request.validate();
			return requireResult(delegate.removeChain(request), request.getTargetId(),
					ChangeRecord.TargetType.CHAIN, ChangeRecord.Op.DELETE, "remove chain");
		}

		@Override
		public PublishResult removeScript(RemoveRuleRequest request) {
			requireRequest(request, "remove script");
			request.validate();
			return requireResult(delegate.removeScript(request), request.getTargetId(),
					ChangeRecord.TargetType.SCRIPT, ChangeRecord.Op.DELETE, "remove script");
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

		private PublishResult requireResult(PublishResult result, String targetId,
				ChangeRecord.TargetType targetType, ChangeRecord.Op operation, String action) {
			if (result == null) {
				throw new RuleStorageException(action + " backend returned null PublishResult");
			}
			if (!targetId.equals(result.getTargetId()) || targetType != result.getTargetType()
					|| operation != result.getOperation()) {
				throw new RuleStorageException(action + " backend returned a PublishResult for unexpected target or operation");
			}
			return result;
		}
	}
}
