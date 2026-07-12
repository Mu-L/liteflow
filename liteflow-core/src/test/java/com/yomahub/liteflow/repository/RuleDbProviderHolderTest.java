package com.yomahub.liteflow.repository;

import com.yomahub.liteflow.exception.ConfigErrorException;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
		FirstProvider first = new FirstProvider();
		SecondProvider second = new SecondProvider();
		ConfigErrorException error = assertThrows(ConfigErrorException.class,
				() -> RuleDbProviderHolder.resolve(Arrays.asList(first, second)));

		assertTrue(error.getMessage().contains(FirstProvider.class.getName()));
		assertTrue(error.getMessage().contains(SecondProvider.class.getName()));
		assertEquals(1, first.closeCalls);
		assertEquals(1, second.closeCalls);
	}

	@Test
	void legacyRepositoryAndProviderCannotBeConfiguredTogether() {
		CountingRepository repository = new CountingRepository();
		CountingRuleProvider provider = new CountingRuleProvider();

		ConfigErrorException error = assertThrows(ConfigErrorException.class,
				() -> RuleRepositoryHolder.resolve(Collections.singletonList(repository), provider));

		assertEquals(1, repository.closeCalls);
		assertEquals(1, provider.closeCalls);
		assertTrue(error.getMessage().contains("both"));
	}

	private static class StubProvider implements RuleDbProvider {
		int closeCalls;
		@Override
		public RuleRepository repository() {
			return null;
		}

		@Override
		public RuleChangeSource changeSource() {
			return null;
		}

		@Override
		public void close() {
			closeCalls++;
		}
	}

	private static final class FirstProvider extends StubProvider {
	}

	private static final class SecondProvider extends StubProvider {
	}

	private static final class CountingRepository implements RuleRepository {
		int closeCalls;
		@Override public com.yomahub.liteflow.repository.vo.RuleManifest fetchManifest() {
			return new com.yomahub.liteflow.repository.vo.RuleManifest();
		}
		@Override public com.yomahub.liteflow.repository.vo.ChainRecord fetchChain(String chainId) { return null; }
		@Override public com.yomahub.liteflow.repository.vo.ScriptRecord fetchScript(String nodeId) { return null; }
		@Override public void close() { closeCalls++; }
	}

	private static final class CountingRuleProvider implements RuleDbProvider {
		int closeCalls;
		@Override public RuleRepository repository() { return new CountingRepository(); }
		@Override public RuleChangeSource changeSource() { return null; }
		@Override public void close() { closeCalls++; }
	}

}
