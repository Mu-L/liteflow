package com.yomahub.liteflow.repository.etcd;

/** Polls a condition until it holds or the deadline expires. */
final class Await {

	private Await() {
	}

	static void until(Check condition) {
		until(condition, 3000);
	}

	static void until(Check condition, long timeoutMillis) {
		long deadline = System.currentTimeMillis() + timeoutMillis;
		while (System.currentTimeMillis() < deadline) {
			if (condition.done()) {
				return;
			}
			try {
				Thread.sleep(10);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				break;
			}
		}
		throw new AssertionError("condition not met within " + timeoutMillis + "ms");
	}

	interface Check {
		boolean done();
	}
}
