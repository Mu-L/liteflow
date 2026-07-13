package com.yomahub.liteflow.repository.etcd;

import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.property.RuleDbEtcdConfig;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.ClientBuilder;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

final class EtcdConnectionManager implements AutoCloseable {

	private final Client client;
	private final boolean owned;

	EtcdConnectionManager(RuleDbEtcdConfig config) {
		this(null, config == null ? null : config.getEndpoints(),
				config == null ? null : config.getUser(), config == null ? null : config.getPassword());
	}

	EtcdConnectionManager(EtcdPublisherConfig config) {
		this(config.getClient(), config.getEndpoints(), config.getUser(), config.getPassword());
	}

	private EtcdConnectionManager(Client supplied, String endpoints, String user, String password) {
		if (supplied != null) {
			this.client = supplied;
			this.owned = false;
			return;
		}
		List<String> endpointList = endpoints(endpoints);
		ClientBuilder builder = Client.builder().endpoints(endpointList.toArray(new String[0]));
		if (StrUtil.isNotBlank(user)) {
			builder.user(bytes(user));
			builder.password(bytes(password == null ? "" : password));
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
				result.add(endpoint.trim());
			}
		}
		if (result.isEmpty()) {
			throw new ConfigErrorException("rule-db etcd endpoints must not be blank");
		}
		return result;
	}

	private static ByteSequence bytes(String value) {
		return ByteSequence.from(value, StandardCharsets.UTF_8);
	}
}
