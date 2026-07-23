package com.yomahub.liteflow.repository.etcd;

import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.property.RuleDbEtcdConfig;
import io.etcd.jetcd.Client;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/** Fail-fast validation and lifecycle of the etcd connection manager. */
class EtcdConnectionManagerTest {

	@Test
	void rejectsBlankEndpoints() {
		assertThrows(ConfigErrorException.class, () -> new EtcdConnectionManager((RuleDbEtcdConfig) null));
		assertThrows(ConfigErrorException.class, () -> new EtcdConnectionManager(config(null)));
		assertThrows(ConfigErrorException.class, () -> new EtcdConnectionManager(config(" ")));
		assertThrows(ConfigErrorException.class, () -> new EtcdConnectionManager(config(",")));
	}

	@Test
	void rejectsMalformedEndpoints() {
		assertThrows(ConfigErrorException.class, () -> new EtcdConnectionManager(config("ftp://host:2379")));
		assertThrows(ConfigErrorException.class, () -> new EtcdConnectionManager(config("host:2379")));
		assertThrows(ConfigErrorException.class,
				() -> new EtcdConnectionManager(config("http://h1:2379, https://h2:2379")));
	}

	@Test
	void acceptsMultipleEndpointsWithWhitespace() {
		EtcdConnectionManager manager = new EtcdConnectionManager(
				config(" http://127.0.0.1:2379 , ,http://127.0.0.1:22379 "));
		assertNotNull(manager.client());
		manager.close();
	}

	@Test
	void rejectsPartialCredentials() {
		RuleDbEtcdConfig config = config("http://127.0.0.1:2379");
		config.setUser("root");
		assertThrows(ConfigErrorException.class, () -> new EtcdConnectionManager(config));

		RuleDbEtcdConfig other = config("http://127.0.0.1:2379");
		other.setPassword("secret");
		assertThrows(ConfigErrorException.class, () -> new EtcdConnectionManager(other));
	}

	@Test
	void rejectsNonPositiveTimeouts() {
		RuleDbEtcdConfig connect = config("http://127.0.0.1:2379");
		connect.setConnectTimeoutMillis(0L);
		assertThrows(ConfigErrorException.class, () -> new EtcdConnectionManager(connect));

		RuleDbEtcdConfig keepaliveTime = config("http://127.0.0.1:2379");
		keepaliveTime.setKeepaliveTimeSeconds(-1L);
		assertThrows(ConfigErrorException.class, () -> new EtcdConnectionManager(keepaliveTime));

		RuleDbEtcdConfig keepaliveTimeout = config("http://127.0.0.1:2379");
		keepaliveTimeout.setKeepaliveTimeoutSeconds(0L);
		assertThrows(ConfigErrorException.class, () -> new EtcdConnectionManager(keepaliveTimeout));
	}

	@Test
	void rejectsPartialClientCertificate() {
		RuleDbEtcdConfig config = tlsConfig();
		config.setClientCertificate(resource("tls/client.pem"));
		assertThrows(ConfigErrorException.class, () -> new EtcdConnectionManager(config));

		RuleDbEtcdConfig other = tlsConfig();
		other.setClientKey(resource("tls/client-key.pem"));
		assertThrows(ConfigErrorException.class, () -> new EtcdConnectionManager(other));
	}

	@Test
	void rejectsTlsCertificatesOnPlainEndpoints() {
		RuleDbEtcdConfig config = config("http://127.0.0.1:2379");
		config.setCaCertificate(resource("tls/ca.pem"));
		assertThrows(ConfigErrorException.class, () -> new EtcdConnectionManager(config));
	}

	@Test
	void rejectsUnreadableCertificateFiles() {
		RuleDbEtcdConfig config = tlsConfig();
		config.setCaCertificate("/no/such/ca.pem");
		assertThrows(ConfigErrorException.class, () -> new EtcdConnectionManager(config));

		RuleDbEtcdConfig key = tlsConfig();
		key.setClientCertificate(resource("tls/client.pem"));
		key.setClientKey("/no/such/client-key.pem");
		assertThrows(ConfigErrorException.class, () -> new EtcdConnectionManager(key));
	}

	@Test
	void buildsClientWithFullPlainConfiguration() {
		RuleDbEtcdConfig config = config("http://127.0.0.1:2379");
		config.setAuthority("etcd-test");
		config.setUser("root");
		config.setPassword("secret");
		config.setConnectTimeoutMillis(1000L);
		config.setKeepaliveTimeSeconds(5L);
		config.setKeepaliveTimeoutSeconds(1L);
		config.setKeepaliveWithoutCalls(false);
		EtcdConnectionManager manager = new EtcdConnectionManager(config);
		assertNotNull(manager.client());
		manager.close();
	}

	@Test
	void buildsTlsClientFromPemFiles() {
		RuleDbEtcdConfig config = tlsConfig();
		config.setCaCertificate(resource("tls/ca.pem"));
		config.setClientCertificate(resource("tls/client.pem"));
		config.setClientKey(resource("tls/client-key.pem"));
		EtcdConnectionManager manager = new EtcdConnectionManager(config);
		assertNotNull(manager.client());
		manager.close();
	}

	@Test
	void suppliedClientIsNotClosedByManager() {
		Client client = mock(Client.class);
		EtcdPublisherConfig config = EtcdPublisherConfig.builder().applicationName("app")
				.client(client).build();
		EtcdConnectionManager manager = new EtcdConnectionManager(config);
		assertSame(client, manager.client());
		manager.close();
		verify(client, never()).close();
	}

	@Test
	void missingClientBeanFallsThroughToEndpointValidation() {
		RuleDbEtcdConfig config = config(null);
		config.setClientBeanName("no-such-bean");
		assertThrows(ConfigErrorException.class, () -> new EtcdConnectionManager(config));
	}

	private RuleDbEtcdConfig config(String endpoints) {
		RuleDbEtcdConfig config = new RuleDbEtcdConfig();
		config.setEndpoints(endpoints);
		config.setConnectTimeoutMillis(null);
		config.setKeepaliveTimeSeconds(null);
		config.setKeepaliveTimeoutSeconds(null);
		config.setKeepaliveWithoutCalls(null);
		return config;
	}

	private RuleDbEtcdConfig tlsConfig() {
		RuleDbEtcdConfig config = config("https://127.0.0.1:2379");
		return config;
	}

	private static String resource(String name) {
		try {
			Path path = Paths.get(EtcdConnectionManagerTest.class.getResource("/" + name).toURI());
			return path.toString();
		}
		catch (URISyntaxException e) {
			throw new IllegalStateException(e);
		}
	}
}
