package com.yomahub.liteflow.repository.nacos;

import cn.hutool.core.util.StrUtil;
import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.exception.NacosException;
import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.log.LFLog;
import com.yomahub.liteflow.log.LFLoggerManager;
import com.yomahub.liteflow.property.RuleDbNacosConfig;
import com.yomahub.liteflow.spi.holder.ContextAwareHolder;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

final class NacosConnectionManager implements AutoCloseable {

	private static final LFLog LOG = LFLoggerManager.getLogger(NacosConnectionManager.class);

	private final ConfigService client;
	private final boolean owned;
	private final AtomicBoolean closed = new AtomicBoolean();

	NacosConnectionManager(RuleDbNacosConfig config) {
		this(executionClient(config), config == null ? null : config.getServerAddr(),
				config == null ? null : config.getNamespace(), config == null ? null : config.getUsername(),
				config == null ? null : config.getPassword(), config == null ? null : config.getAccessKey(),
				config == null ? null : config.getSecretKey(), config == null ? null : config.getTimeoutMillis());
	}

	NacosConnectionManager(NacosPublisherConfig config) {
		this(config.getConfigService(), config.getServerAddr(), config.getNamespace(), config.getUsername(),
				config.getPassword(), config.getAccessKey(), config.getSecretKey(), config.getTimeoutMillis());
	}

	private NacosConnectionManager(ConfigService supplied, String serverAddr, String namespace,
			String username, String password, String accessKey, String secretKey, Long timeoutMillis) {
		validateClientApi();
		validateTimeout(timeoutMillis);
		validateCredentials(username, password, accessKey, secretKey);
		if (supplied != null) {
			this.client = supplied;
			this.owned = false;
			return;
		}
		if (StrUtil.isBlank(serverAddr)) {
			throw new ConfigErrorException(
					"rule-db nacos requires a ConfigService bean or liteflow.rule-db.nacos.server-addr");
		}
		Properties properties = new Properties();
		properties.put(PropertyKeyConst.SERVER_ADDR, serverAddr.trim());
		properties.put(PropertyKeyConst.NAMESPACE, StrUtil.nullToEmpty(namespace));
		properties.put(PropertyKeyConst.CONFIG_REQUEST_TIMEOUT, String.valueOf(timeoutMillis));
		putIfPresent(properties, PropertyKeyConst.USERNAME, username);
		putIfPresent(properties, PropertyKeyConst.PASSWORD, password);
		putIfPresent(properties, PropertyKeyConst.ACCESS_KEY, accessKey);
		putIfPresent(properties, PropertyKeyConst.SECRET_KEY, secretKey);
		try {
			this.client = NacosFactory.createConfigService(properties);
			this.owned = true;
		}
		catch (NacosException e) {
			throw new ConfigErrorException("rule-db nacos cannot create ConfigService: " + e.getMessage());
		}
	}

	ConfigService client() {
		return client;
	}

	@Override
	public void close() {
		if (!owned || !closed.compareAndSet(false, true)) {
			return;
		}
		try {
			client.shutDown();
		}
		catch (NacosException | RuntimeException e) {
			LOG.warn("rule-db nacos ConfigService shutdown failed: {}", e.getMessage());
		}
	}

	private static ConfigService executionClient(RuleDbNacosConfig config) {
		if (config == null) {
			return null;
		}
		if (StrUtil.isNotBlank(config.getConfigServiceBeanName())) {
			try {
				return ContextAwareHolder.loadContextAware().getBean(config.getConfigServiceBeanName());
			}
			catch (Exception e) {
				throw new ConfigErrorException("rule-db nacos ConfigService bean["
						+ config.getConfigServiceBeanName() + "] is not available");
			}
		}
		try {
			return ContextAwareHolder.loadContextAware().getBean(ConfigService.class);
		}
		catch (Exception ignored) {
			return null;
		}
	}

	private static void validateTimeout(Long timeoutMillis) {
		if (timeoutMillis == null || timeoutMillis <= 0) {
			throw new ConfigErrorException("rule-db nacos timeout-millis must be greater than zero");
		}
	}

	private static void validateClientApi() {
		try {
			ConfigService.class.getMethod("publishConfigCas", String.class, String.class,
					String.class, String.class);
		}
		catch (NoSuchMethodException e) {
			throw new ConfigErrorException(
					"rule-db nacos requires nacos-client 2.x with publishConfigCas support");
		}
	}

	private static void validateCredentials(String username, String password,
			String accessKey, String secretKey) {
		if (StrUtil.isBlank(username) != StrUtil.isBlank(password)) {
			throw new ConfigErrorException("rule-db nacos username and password must be configured together");
		}
		if (StrUtil.isBlank(accessKey) != StrUtil.isBlank(secretKey)) {
			throw new ConfigErrorException("rule-db nacos access-key and secret-key must be configured together");
		}
		if (StrUtil.isNotBlank(username) && StrUtil.isNotBlank(accessKey)) {
			throw new ConfigErrorException(
					"rule-db nacos username/password and access-key/secret-key are mutually exclusive");
		}
	}

	private static void putIfPresent(Properties properties, String key, String value) {
		if (StrUtil.isNotBlank(value)) {
			properties.put(key, value);
		}
	}
}
