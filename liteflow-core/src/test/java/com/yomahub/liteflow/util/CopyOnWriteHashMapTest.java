package com.yomahub.liteflow.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class CopyOnWriteHashMapTest {

	@Test
	public void testConditionalOperationsUseVisibleView() {
		assertConditionalOperations(new CopyOnWriteHashMap<>());
	}

	@Test
	public void testConditionalOperationsUseConcurrentMapStorage() {
		assertConditionalOperations(new ConcurrentHashMap<>());
	}

	private void assertConditionalOperations(ConcurrentMap<String, Object> map) {
		Object first = new Object();
		Object second = new Object();
		map.put("key", first);

		Assertions.assertTrue(map.replace("key", first, second));
		Assertions.assertSame(second, map.get("key"));
		Assertions.assertFalse(map.replace("key", first, new Object()));
		Assertions.assertTrue(map.remove("key", second));
		Assertions.assertFalse(map.containsKey("key"));
	}
}
