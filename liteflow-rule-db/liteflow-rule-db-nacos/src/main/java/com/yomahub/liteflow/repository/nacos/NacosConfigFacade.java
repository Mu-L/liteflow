package com.yomahub.liteflow.repository.nacos;

import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.Listener;
import com.alibaba.nacos.api.exception.NacosException;
import com.yomahub.liteflow.publisher.exception.RuleStorageException;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

interface NacosConfigFacade {

	String get(String dataId, String group, long timeoutMillis);

	boolean publishCas(String dataId, String group, String content, String expectedMd5);

	Subscription subscribe(String dataId, String group, long timeoutMillis, Consumer<String> consumer);

	final class Subscription implements AutoCloseable {
		private final String initialContent;
		private final Runnable closeAction;
		private final AtomicBoolean closed = new AtomicBoolean();

		Subscription(String initialContent, Runnable closeAction) {
			this.initialContent = initialContent;
			this.closeAction = closeAction;
		}

		String initialContent() {
			return initialContent;
		}

		@Override
		public void close() {
			if (closed.compareAndSet(false, true)) {
				closeAction.run();
			}
		}
	}
}

final class ClientNacosConfigFacade implements NacosConfigFacade {

	private final ConfigService client;

	ClientNacosConfigFacade(ConfigService client) {
		this.client = client;
	}

	@Override
	public String get(String dataId, String group, long timeoutMillis) {
		try {
			return client.getConfig(dataId, group, timeoutMillis);
		}
		catch (NacosException e) {
			throw storage("read", dataId, group, e);
		}
	}

	@Override
	public boolean publishCas(String dataId, String group, String content, String expectedMd5) {
		try {
			return client.publishConfigCas(dataId, group, content, expectedMd5);
		}
		catch (NacosException e) {
			throw storage("CAS publish", dataId, group, e);
		}
	}

	@Override
	public Subscription subscribe(String dataId, String group, long timeoutMillis, Consumer<String> consumer) {
		Listener listener = new Listener() {
			@Override
			public Executor getExecutor() {
				return null;
			}

			@Override
			public void receiveConfigInfo(String configInfo) {
				consumer.accept(configInfo);
			}
		};
		try {
			String initial = client.getConfigAndSignListener(dataId, group, timeoutMillis, listener);
			return new Subscription(initial, () -> client.removeListener(dataId, group, listener));
		}
		catch (NacosException e) {
			throw storage("subscribe", dataId, group, e);
		}
	}

	private RuleStorageException storage(String operation, String dataId, String group, NacosException cause) {
		return new RuleStorageException("Nacos " + operation + " failed for dataId[" + dataId
				+ "] group[" + group + "]: " + cause.getMessage(), cause);
	}
}
