package com.yomahub.liteflow.repository.nacos;

import cn.hutool.crypto.SecureUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

final class FakeNacosConfig implements NacosConfigFacade {

	private final List<Consumer<String>> listeners = new ArrayList<>();
	private String content;
	private int rejectedCas;
	private int droppedNotifications;

	@Override
	public synchronized String get(String dataId, String group, long timeoutMillis) {
		return content;
	}

	@Override
	public boolean publishCas(String dataId, String group, String next, String expectedMd5) {
		List<Consumer<String>> notifying;
		synchronized (this) {
			if (rejectedCas > 0) {
				rejectedCas--;
				return false;
			}
			String actual = SecureUtil.md5(content == null ? "" : content);
			if (!actual.equals(expectedMd5)) {
				return false;
			}
			content = next;
			if (droppedNotifications > 0) {
				droppedNotifications--;
				return true;
			}
			notifying = new ArrayList<>(listeners);
		}
		for (Consumer<String> listener : notifying) {
			listener.accept(next);
		}
		return true;
	}

	@Override
	public synchronized Subscription subscribe(String dataId, String group,
			long timeoutMillis, Consumer<String> consumer) {
		listeners.add(consumer);
		return new Subscription(content, () -> remove(consumer));
	}

	synchronized String content() {
		return content;
	}

	synchronized int listenerCount() {
		return listeners.size();
	}

	synchronized void rejectNextCas() {
		rejectedCas++;
	}

	synchronized void dropNextNotification() {
		droppedNotifications++;
	}

	void emit(String value) {
		List<Consumer<String>> notifying;
		synchronized (this) {
			notifying = new ArrayList<>(listeners);
		}
		for (Consumer<String> listener : notifying) {
			listener.accept(value);
		}
	}

	private synchronized void remove(Consumer<String> consumer) {
		listeners.remove(consumer);
	}
}
