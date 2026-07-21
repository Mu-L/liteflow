package com.yomahub.liteflow.repository.etcd;

import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.property.RuleDbEtcdConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EtcdConnectionConfigurationTest {

	@Test
	void acceptsMultipleHttpEndpointsAndAppliesConnectionSettings() {
		RuleDbEtcdConfig config = config("http://127.0.0.1:2379,http://127.0.0.1:22379");
		try (EtcdConnectionManager connection = new EtcdConnectionManager(config)) {
			assertNotNull(connection.client());
		}
	}

	@Test
	void rejectsPartialCredentials() {
		RuleDbEtcdConfig config = config("http://127.0.0.1:2379");
		config.setUser("root");

		assertThrows(ConfigErrorException.class, () -> new EtcdConnectionManager(config));
	}

	@Test
	void rejectsInvalidTimeouts() {
		RuleDbEtcdConfig config = config("http://127.0.0.1:2379");
		config.setConnectTimeoutMillis(0L);

		assertThrows(ConfigErrorException.class, () -> new EtcdConnectionManager(config));
	}

	@Test
	void rejectsMixedTlsAndPlaintextEndpoints() {
		RuleDbEtcdConfig config = config("https://127.0.0.1:2379,http://127.0.0.1:22379");

		assertThrows(ConfigErrorException.class, () -> new EtcdConnectionManager(config));
	}

	@Test
	void rejectsPartialMutualTlsAndCertificatesOnPlaintext() {
		RuleDbEtcdConfig partial = config("https://127.0.0.1:2379");
		partial.setClientCertificate("client.crt");
		assertThrows(ConfigErrorException.class, () -> new EtcdConnectionManager(partial));

		RuleDbEtcdConfig plaintext = config("http://127.0.0.1:2379");
		plaintext.setCaCertificate("ca.crt");
		assertThrows(ConfigErrorException.class, () -> new EtcdConnectionManager(plaintext));
	}

	@Test
	void rejectsMissingCertificateFiles() {
		RuleDbEtcdConfig config = config("https://127.0.0.1:2379");
		config.setCaCertificate("/path/that/does/not/exist/ca.crt");

		assertThrows(ConfigErrorException.class, () -> new EtcdConnectionManager(config));
	}

	private RuleDbEtcdConfig config(String endpoints) {
		RuleDbEtcdConfig config = new RuleDbEtcdConfig();
		config.setEndpoints(endpoints);
		return config;
	}
}
