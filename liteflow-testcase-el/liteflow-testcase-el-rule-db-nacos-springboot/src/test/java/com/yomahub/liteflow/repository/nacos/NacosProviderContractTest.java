package com.yomahub.liteflow.repository.nacos;

import com.alibaba.nacos.api.config.ConfigService;
import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.property.RuleDbNacosConfig;
import com.yomahub.liteflow.publisher.PublisherBackend;
import com.yomahub.liteflow.publisher.RulePublisher;
import com.yomahub.liteflow.publisher.RulePublisherFactory;
import com.yomahub.liteflow.publisher.exception.PublisherConfigurationException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class NacosProviderContractTest {

	@Test
	void typedConfigAndServiceLoaderCreatePublisherWithBorrowedClient() throws Exception {
		ConfigService client = mock(ConfigService.class);
		NacosPublisherConfig config = NacosPublisherConfig.builder().applicationName("app")
				.group("RULES").dataIdPrefix("lf").configService(client).build();

		assertEquals(PublisherBackend.NACOS, config.backend());
		NacosRulePublisherProvider provider = new NacosRulePublisherProvider();
		assertTrue(provider.supports(config));
		try (RulePublisher ignored = RulePublisherFactory.create(config)) {
			// ServiceLoader resolved the Nacos provider and borrowed the supplied client.
		}
		verify(client, never()).shutDown();
	}

	@Test
	void providerRejectsIncompleteConnectionAndInvalidCatalogCoordinates() {
		NacosRulePublisherProvider provider = new NacosRulePublisherProvider();
		assertThrows(PublisherConfigurationException.class,
				() -> provider.create(NacosPublisherConfig.builder().applicationName("app").build()));
		assertThrows(PublisherConfigurationException.class,
				() -> provider.create(NacosPublisherConfig.builder().applicationName("bad/app")
						.configService(mock(ConfigService.class)).build()));
		assertThrows(PublisherConfigurationException.class,
				() -> provider.create(NacosPublisherConfig.builder().applicationName("app")
						.group(" ").configService(mock(ConfigService.class)).build()));
	}

	@Test
	void connectionValidationRunsBeforeCreatingOrUsingClient() {
		RuleDbNacosConfig timeout = new RuleDbNacosConfig();
		timeout.setTimeoutMillis(0L);
		assertThrows(ConfigErrorException.class, () -> new NacosConnectionManager(timeout));

		RuleDbNacosConfig credentials = new RuleDbNacosConfig();
		credentials.setServerAddr("127.0.0.1:8848");
		credentials.setUsername("user");
		assertThrows(ConfigErrorException.class, () -> new NacosConnectionManager(credentials));

		RuleDbNacosConfig cloud = new RuleDbNacosConfig();
		cloud.setServerAddr("127.0.0.1:8848");
		cloud.setAccessKey("ak");
		cloud.setSecretKey("sk");
		cloud.setUsername("user");
		cloud.setPassword("pass");
		assertThrows(ConfigErrorException.class, () -> new NacosConnectionManager(cloud));
	}

	@Test
	void coreConfigExposesReleaseDefaults() {
		RuleDbNacosConfig config = new RuleDbNacosConfig();
		assertEquals("LITEFLOW_RULE_DB", config.getGroup());
		assertEquals("liteflow-rule-db", config.getDataIdPrefix());
		assertEquals(3000L, config.getTimeoutMillis());
	}
}
