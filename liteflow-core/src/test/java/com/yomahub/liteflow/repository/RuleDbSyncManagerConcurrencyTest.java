package com.yomahub.liteflow.repository;

import com.yomahub.liteflow.repository.vo.RuleManifest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RuleDbSyncManagerConcurrencyTest {

	@AfterEach
	void tearDown() {
		RuleDbSyncManager.stop();
	}

	@Test
	void manualPollDoesNotDeadlockWithBackgroundCallback() {
		assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
			BlockingPollingSource source = new BlockingPollingSource();
			RuleDbSyncManager.open(new StubProvider(source));
			RuleDbSyncManager.activate(0L);

			ExecutorService executor = Executors.newFixedThreadPool(2, runnable -> {
				Thread thread = new Thread(runnable, "rule-db-deadlock-regression");
				thread.setDaemon(true);
				return thread;
			});
			try {
				Future<?> background = executor.submit(source::pollOnce);
				assertTrue(source.firstPollEntered.await(2, TimeUnit.SECONDS));
				Future<?> manual = executor.submit(RuleDbSyncManager::pollOnce);
				source.allowCallback.countDown();

				background.get(2, TimeUnit.SECONDS);
				manual.get(2, TimeUnit.SECONDS);
			}
			finally {
				executor.shutdownNow();
			}
		});
	}

	private static final class StubProvider implements RuleDbProvider {
		private final BlockingPollingSource source;

		private StubProvider(BlockingPollingSource source) {
			this.source = source;
		}

		@Override
		public RuleRepository repository() {
			return new RuleRepository() {
				@Override
				public RuleManifest fetchManifest() {
					RuleManifest manifest = new RuleManifest();
					manifest.setChains(Collections.emptyList());
					manifest.setScripts(Collections.emptyList());
					manifest.setLatestSeq(0L);
					return manifest;
				}

				@Override
				public com.yomahub.liteflow.repository.vo.ChainRecord fetchChain(String chainId) {
					return null;
				}

				@Override
				public com.yomahub.liteflow.repository.vo.ScriptRecord fetchScript(String nodeId) {
					return null;
				}
			};
		}

		@Override
		public RuleChangeSource changeSource() {
			return source;
		}
	}

	private static final class BlockingPollingSource
			implements RuleChangeSource, ManualPollingChangeSource {

		private final Object pollMonitor = new Object();
		private final AtomicInteger calls = new AtomicInteger();
		private final CountDownLatch firstPollEntered = new CountDownLatch(1);
		private final CountDownLatch allowCallback = new CountDownLatch(1);
		private RuleChangeListener listener;

		@Override
		public void open(RuleChangeListener listener) {
			this.listener = listener;
		}

		@Override
		public void activate(long baselineSeq) {
		}

		@Override
		public ChangeSourceHealth health() {
			return ChangeSourceHealth.starting();
		}

		@Override
		public void pollOnce() {
			synchronized (pollMonitor) {
				if (calls.incrementAndGet() == 1) {
					firstPollEntered.countDown();
					try {
						assertTrue(allowCallback.await(2, TimeUnit.SECONDS));
					}
					catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						throw new AssertionError(e);
					}
					listener.onReconcileRequired();
				}
			}
		}
	}
}
