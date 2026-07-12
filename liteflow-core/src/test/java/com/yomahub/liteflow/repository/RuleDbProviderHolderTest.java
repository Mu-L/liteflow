package com.yomahub.liteflow.repository;

import com.yomahub.liteflow.exception.ConfigErrorException;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RuleDbProviderHolderTest {

	@Test
	void resolveHandlesNoProviderAndOneProvider() {
		assertNull(RuleDbProviderHolder.resolve(Collections.emptyList()));

		StubProvider provider = new StubProvider();
		assertSame(provider, RuleDbProviderHolder.resolve(Collections.singletonList(provider)));
	}

	@Test
	void resolveReportsAllConflictingProviderTypes() {
		ConfigErrorException error = assertThrows(ConfigErrorException.class,
				() -> RuleDbProviderHolder.resolve(Arrays.asList(new FirstProvider(), new SecondProvider())));

		assertTrue(error.getMessage().contains(FirstProvider.class.getName()));
		assertTrue(error.getMessage().contains(SecondProvider.class.getName()));
	}

	private static class StubProvider implements RuleDbProvider {
		@Override
		public RuleRepository repository() {
			return null;
		}

		@Override
		public RuleChangeSource changeSource() {
			return null;
		}
	}

	private static final class FirstProvider extends StubProvider {
	}

	private static final class SecondProvider extends StubProvider {
	}

}
