package com.yomahub.liteflow.repository.etcd;

import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.property.RuleDbEtcdConfig;
import com.yomahub.liteflow.spi.holder.ContextAwareHolder;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.ClientBuilder;

import javax.net.ssl.SSLException;
import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

final class EtcdConnectionManager implements AutoCloseable {

	private final Client client;
	private final boolean owned;

	EtcdConnectionManager(RuleDbEtcdConfig config) {
		this(executionClient(config), config == null ? null : config.getEndpoints(),
				config == null ? null : config.getUser(), config == null ? null : config.getPassword(),
				config == null ? null : config.getCaCertificate(),
				config == null ? null : config.getClientCertificate(),
				config == null ? null : config.getClientKey(), config == null ? null : config.getAuthority(),
				config == null ? null : config.getConnectTimeoutMillis(),
				config == null ? null : config.getKeepaliveTimeSeconds(),
				config == null ? null : config.getKeepaliveTimeoutSeconds(),
				config == null ? null : config.getKeepaliveWithoutCalls());
	}

	EtcdConnectionManager(EtcdPublisherConfig config) {
		this(config.getClient(), config.getEndpoints(), config.getUser(), config.getPassword(),
				config.getCaCertificate(), config.getClientCertificate(), config.getClientKey(),
				config.getAuthority(), config.getConnectTimeoutMillis(), config.getKeepaliveTimeSeconds(),
				config.getKeepaliveTimeoutSeconds(), config.getKeepaliveWithoutCalls());
	}

	private EtcdConnectionManager(Client supplied, String endpoints, String user, String password,
			String caCertificate, String clientCertificate, String clientKey, String authority,
			Long connectTimeoutMillis, Long keepaliveTimeSeconds, Long keepaliveTimeoutSeconds,
			Boolean keepaliveWithoutCalls) {
		if (supplied != null) {
			this.client = supplied;
			this.owned = false;
			return;
		}
		List<String> endpointList = endpoints(endpoints);
		validateCredentials(user, password);
		validatePositive("connect-timeout-millis", connectTimeoutMillis);
		validatePositive("keepalive-time-seconds", keepaliveTimeSeconds);
		validatePositive("keepalive-timeout-seconds", keepaliveTimeoutSeconds);
		boolean tls = usesTls(endpointList);
		validateTlsFiles(tls, caCertificate, clientCertificate, clientKey);

		ClientBuilder builder = Client.builder();
		if (StrUtil.isNotBlank(authority)) {
			builder.authority(authority.trim());
		}
		builder.endpoints(endpointList.toArray(new String[0]));
		if (StrUtil.isNotBlank(user)) {
			builder.user(bytes(user));
			builder.password(bytes(password));
		}
		if (connectTimeoutMillis != null) {
			builder.connectTimeout(Duration.ofMillis(connectTimeoutMillis));
		}
		if (keepaliveTimeSeconds != null) {
			builder.keepaliveTime(Duration.ofSeconds(keepaliveTimeSeconds));
		}
		if (keepaliveTimeoutSeconds != null) {
			builder.keepaliveTimeout(Duration.ofSeconds(keepaliveTimeoutSeconds));
		}
		if (keepaliveWithoutCalls != null) {
			builder.keepaliveWithoutCalls(keepaliveWithoutCalls);
		}
		if (tls) {
			configureTls(builder, caCertificate, clientCertificate, clientKey);
		}
		this.client = builder.build();
		this.owned = true;
	}

	Client client() {
		return client;
	}

	@Override
	public void close() {
		if (owned) {
			client.close();
		}
	}

	private static List<String> endpoints(String value) {
		if (StrUtil.isBlank(value)) {
			throw new ConfigErrorException("rule-db etcd endpoints must not be blank");
		}
		List<String> result = new ArrayList<>();
		for (String endpoint : value.split(",")) {
			if (StrUtil.isNotBlank(endpoint)) {
				String normalized = endpoint.trim();
				try {
					URI uri = URI.create(normalized);
					if (uri.getHost() == null || (!"http".equalsIgnoreCase(uri.getScheme())
							&& !"https".equalsIgnoreCase(uri.getScheme()))) {
						throw new IllegalArgumentException();
					}
				}
				catch (IllegalArgumentException e) {
					throw new ConfigErrorException("rule-db etcd endpoint must use http:// or https://: " + normalized);
				}
				result.add(normalized);
			}
		}
		if (result.isEmpty()) {
			throw new ConfigErrorException("rule-db etcd endpoints must not be blank");
		}
		return result;
	}

	private static boolean usesTls(List<String> endpoints) {
		boolean tls = endpoints.get(0).regionMatches(true, 0, "https://", 0, 8);
		for (String endpoint : endpoints) {
			boolean endpointTls = endpoint.regionMatches(true, 0, "https://", 0, 8);
			if (endpointTls != tls) {
				throw new ConfigErrorException("rule-db etcd endpoints must not mix http and https");
			}
		}
		return tls;
	}

	private static void validateCredentials(String user, String password) {
		if (StrUtil.isBlank(user) != StrUtil.isBlank(password)) {
			throw new ConfigErrorException("rule-db etcd user and password must be configured together");
		}
	}

	private static void validatePositive(String name, Long value) {
		if (value != null && value <= 0) {
			throw new ConfigErrorException("rule-db etcd " + name + " must be greater than zero");
		}
	}

	private static void validateTlsFiles(boolean tls, String caCertificate,
			String clientCertificate, String clientKey) {
		if (StrUtil.isBlank(clientCertificate) != StrUtil.isBlank(clientKey)) {
			throw new ConfigErrorException(
					"rule-db etcd client-certificate and client-key must be configured together");
		}
		if (!tls && (StrUtil.isNotBlank(caCertificate) || StrUtil.isNotBlank(clientCertificate))) {
			throw new ConfigErrorException("rule-db etcd TLS certificates require https endpoints");
		}
		validateReadableFile("ca-certificate", caCertificate);
		validateReadableFile("client-certificate", clientCertificate);
		validateReadableFile("client-key", clientKey);
	}

	private static void validateReadableFile(String name, String value) {
		if (StrUtil.isBlank(value)) {
			return;
		}
		Path path = Paths.get(value.trim());
		if (!Files.isRegularFile(path) || !Files.isReadable(path)) {
			throw new ConfigErrorException("rule-db etcd " + name + " is not a readable file: " + value);
		}
	}

	private static void configureTls(ClientBuilder builder, String caCertificate,
			String clientCertificate, String clientKey) {
		try {
			builder.sslContext(ssl -> {
				if (StrUtil.isNotBlank(caCertificate)) {
					ssl.trustManager(new File(caCertificate.trim()));
				}
				if (StrUtil.isNotBlank(clientCertificate)) {
					ssl.keyManager(new File(clientCertificate.trim()), new File(clientKey.trim()));
				}
			});
		}
		catch (SSLException e) {
			throw new ConfigErrorException("rule-db etcd cannot build TLS context: " + e.getMessage());
		}
	}

	private static Client executionClient(RuleDbEtcdConfig config) {
		if (config == null || StrUtil.isBlank(config.getClientBeanName())) {
			return null;
		}
		try {
			return ContextAwareHolder.loadContextAware().getBean(config.getClientBeanName());
		}
		catch (Exception e) {
			throw new ConfigErrorException("rule-db etcd Client bean["
					+ config.getClientBeanName() + "] is not available");
		}
	}

	private static ByteSequence bytes(String value) {
		return ByteSequence.from(value, StandardCharsets.UTF_8);
	}
}
