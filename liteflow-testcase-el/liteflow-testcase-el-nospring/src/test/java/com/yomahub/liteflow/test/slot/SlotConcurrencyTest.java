package com.yomahub.liteflow.test.slot;

import com.yomahub.liteflow.flow.element.Chain;
import com.yomahub.liteflow.slot.Slot;
import com.yomahub.liteflow.test.BaseTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Slot中chain实例列表的并发安全测试
 * <p>
 * 对应场景(issue #IDB16L):WHEN并行分支里执行子chain(写chainInstance),
 * 与嵌套WHEN构建线程池时对chainInstance的流式遍历(读)并发发生,
 * 旧实现在此场景下会抛出ConcurrentModificationException
 */
public class SlotConcurrencyTest extends BaseTest {

	@Test
	public void testConcurrentChainInstanceReadWrite() throws Exception {
		Slot slot = new Slot();
		// 预置一个chain,确保结构已初始化,排除初始化时机的干扰
		Chain firstChain = new Chain("firstChain");
		slot.addChainInstance(firstChain);

		int writeTimes = 20000;
		ExecutorService pool = Executors.newFixedThreadPool(2);
		CountDownLatch startLatch = new CountDownLatch(1);
		AtomicBoolean writing = new AtomicBoolean(true);
		AtomicReference<Throwable> errorRef = new AtomicReference<>();

		// 写线程:模拟 Chain.execute 不断向 slot 追加 chain 实例
		pool.submit(() -> {
			await(startLatch);
			try {
				for (int i = 0; i < writeTimes && errorRef.get() == null; i++) {
					slot.addChainInstance(new Chain("chain_" + i));
				}
			} catch (Throwable t) {
				errorRef.compareAndSet(null, t);
			} finally {
				writing.set(false);
			}
		});

		// 读线程:模拟 buildExecutorService -> getCurrentChainInstance 的遍历
		// 查询一个不存在的id,强制全量遍历,放大并发窗口
		pool.submit(() -> {
			await(startLatch);
			try {
				while (writing.get() && errorRef.get() == null) {
					slot.getCurrentChainInstance("notExistChainId");
				}
			} catch (Throwable t) {
				errorRef.compareAndSet(null, t);
			}
		});

		startLatch.countDown();

		pool.shutdown();
		Assertions.assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS), "并发读写未在预期时间内结束");

		Throwable error = errorRef.get();
		Assertions.assertNull(error, "并发读写chainInstance时出现异常: " + (error == null ? "" : error.toString()));

		// 功能断言:依旧能拿到最先加入的实例,查不存在的id返回null
		Assertions.assertSame(firstChain, slot.getCurrentChainInstance("firstChain"));
		Assertions.assertNull(slot.getCurrentChainInstance("notExistChainId"));
	}

	/**
	 * 语义守护:同一个chainId重复执行(如循环场景),应保留最先加入的实例
	 * 这与旧实现 stream().filter().findFirst() 的行为保持一致
	 */
	@Test
	public void testFirstChainInstanceWinsOnDuplicateId() {
		Slot slot = new Slot();
		Chain first = new Chain("dupChain");
		Chain second = new Chain("dupChain");

		slot.addChainInstance(first);
		slot.addChainInstance(second);

		Assertions.assertSame(first, slot.getCurrentChainInstance("dupChain"));
	}

	private void await(CountDownLatch latch) {
		try {
			latch.await();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

}
