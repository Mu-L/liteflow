package com.yomahub.liteflow.repository.etcd;

import com.yomahub.liteflow.publisher.PublisherBackend;
import com.yomahub.liteflow.publisher.RulePublisher;
import com.yomahub.liteflow.publisher.RulePublisherConfig;
import com.yomahub.liteflow.publisher.exception.PublisherConfigurationException;
import io.etcd.jetcd.Client;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Validation layering of the publisher provider and the config builder. */
class EtcdRulePublisherProviderTest {

	private final EtcdRulePublisherProvider provider = new EtcdRulePublisherProvider();

	@Test
	void supportsOnlyEtcdBackendConfigs() {
		assertTrue(provider.supports(EtcdPublisherConfig.builder().applicationName("app").build()));
		assertFalse(provider.supports(otherConfig()));
		assertFalse(provider.supports(null));
	}

	@Test
	void createValidatesBeforeTouchingEtcd() {
		assertThrows(PublisherConfigurationException.class, () -> provider.create(otherConfig()));
		assertThrows(PublisherConfigurationException.class,
				() -> provider.create(EtcdPublisherConfig.builder().applicationName("app").build()));
		assertThrows(PublisherConfigurationException.class,
				() -> provider.create(EtcdPublisherConfig.builder().applicationName(" ")
						.endpoints("http://127.0.0.1:2379").build()));
		assertThrows(PublisherConfigurationException.class,
				() -> provider.create(EtcdPublisherConfig.builder().applicationName("app")
						.endpoints("http://127.0.0.1:2379").rootPath(" ").build()));
	}

	@Test
	void createsPublisherWithSuppliedClient() {
		Client client = mock(Client.class);
		EtcdPublisherConfig config = EtcdPublisherConfig.builder().applicationName("app")
				.client(client).rootPath("/lf").build();
		RulePublisher publisher = provider.create(config);
		publisher.close();
		verify(client, never()).close();
	}

	@Test
	void failedInitializationDoesNotCloseSuppliedClient() {
		Client client = mock(Client.class);
		when(client.getKVClient()).thenThrow(new IllegalStateException("cannot create KV client"));
		EtcdPublisherConfig config = EtcdPublisherConfig.builder().applicationName("app")
				.client(client).rootPath("/lf").build();

		assertThrows(IllegalStateException.class, () -> provider.create(config));
		verify(client, never()).close();
	}

	@Test
	void builderExposesEverySetting() {
		Client client = mock(Client.class);
		EtcdPublisherConfig config = EtcdPublisherConfig.builder()
				.applicationName("app")
				.endpoints("http://127.0.0.1:2379")
				.user("root")
				.password("secret")
				.rootPath("/rules")
				.caCertificate("ca.pem")
				.clientCertificate("client.pem")
				.clientKey("client-key.pem")
				.authority("etcd")
				.connectTimeoutMillis(100L)
				.keepaliveTimeSeconds(2L)
				.keepaliveTimeoutSeconds(1L)
				.keepaliveWithoutCalls(false)
				.client(client)
				.build();
		assertEquals("app", config.applicationName());
		assertEquals(PublisherBackend.ETCD, config.backend());
		assertEquals("http://127.0.0.1:2379", config.getEndpoints());
		assertEquals("root", config.getUser());
		assertEquals("secret", config.getPassword());
		assertEquals("/rules", config.getRootPath());
		assertEquals("ca.pem", config.getCaCertificate());
		assertEquals("client.pem", config.getClientCertificate());
		assertEquals("client-key.pem", config.getClientKey());
		assertEquals("etcd", config.getAuthority());
		assertEquals(100L, config.getConnectTimeoutMillis());
		assertEquals(2L, config.getKeepaliveTimeSeconds());
		assertEquals(1L, config.getKeepaliveTimeoutSeconds());
		assertEquals(false, config.getKeepaliveWithoutCalls());
		assertTrue(config.getClient() == client);
	}

	private RulePublisherConfig otherConfig() {
		return new RulePublisherConfig() {
			@Override
			public String applicationName() {
				return "app";
			}

			@Override
			public PublisherBackend backend() {
				return PublisherBackend.SQL;
			}
		};
	}
}
