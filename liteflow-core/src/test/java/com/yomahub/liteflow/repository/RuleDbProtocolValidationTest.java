package com.yomahub.liteflow.repository;

import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.repository.vo.ChainMeta;
import com.yomahub.liteflow.repository.vo.ChainRecord;
import com.yomahub.liteflow.repository.vo.RuleManifest;
import com.yomahub.liteflow.repository.vo.ScriptRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RuleDbProtocolValidationTest {

	@AfterEach
	void cleanup() {
		RuleDbSyncManager.stop();
	}

	@Test
	void providerMustExposeChangeSource() {
		NullSourceProvider provider = new NullSourceProvider();

		assertThrows(ConfigErrorException.class, () -> RuleDbSyncManager.open(provider));
		assertEquals(1, provider.closeCalls);
	}

	@Test
	void malformedManifestIsRejectedBeforeUse() {
		RuleManifest manifest = new RuleManifest();
		manifest.setChains(Collections.singletonList(new ChainMeta("", 0, null)));

		assertThrows(ConfigErrorException.class, () -> RuleDbRuntime.validateManifest(manifest));
	}

	private static final class NullSourceProvider implements RuleDbProvider {
		private int closeCalls;

		@Override
		public RuleRepository repository() {
			return new EmptyRepository();
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

	private static final class EmptyRepository implements RuleRepository {
		@Override
		public RuleManifest fetchManifest() {
			return new RuleManifest();
		}

		@Override
		public ChainRecord fetchChain(String chainId) {
			return null;
		}

		@Override
		public ScriptRecord fetchScript(String nodeId) {
			return null;
		}
	}
}
