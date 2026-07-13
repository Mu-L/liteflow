package com.yomahub.liteflow.repository.etcd;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchResponse;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

interface EtcdWatchFacade {

	Handle watch(String prefix, long startRevision, Listener listener);

	interface Handle extends AutoCloseable {
		@Override
		void close();
	}

	interface Listener {
		void onEvents(List<Event> events);
		void onError(Throwable error);
	}

	final class Event {
		private final String key;
		private final String value;
		private final long revision;
		private final boolean delete;

		Event(String key, String value, long revision, boolean delete) {
			this.key = key;
			this.value = value;
			this.revision = revision;
			this.delete = delete;
		}

		String key() { return key; }
		String value() { return value; }
		long revision() { return revision; }
		boolean delete() { return delete; }
	}
}

final class JetcdWatchFacade implements EtcdWatchFacade {

	private final Watch watch;

	JetcdWatchFacade(Watch watch) {
		this.watch = watch;
	}

	@Override
	public Handle watch(String prefix, long startRevision, Listener listener) {
		ByteSequence key = bytes(prefix);
		WatchOption.Builder option = WatchOption.newBuilder().withPrefix(key);
		if (startRevision > 0) {
			option.withRevision(startRevision);
		}
		Watch.Watcher watcher = watch.watch(key, option.build(), Watch.listener(
				response -> listener.onEvents(events(response)), listener::onError));
		return watcher::close;
	}

	private List<Event> events(WatchResponse response) {
		List<Event> result = new ArrayList<>();
		for (WatchEvent event : response.getEvents()) {
			boolean delete = event.getEventType() == WatchEvent.EventType.DELETE;
			result.add(new Event(text(event.getKeyValue().getKey()),
					delete ? null : text(event.getKeyValue().getValue()),
					event.getKeyValue().getModRevision(), delete));
		}
		return result;
	}

	private static ByteSequence bytes(String value) {
		return ByteSequence.from(value, StandardCharsets.UTF_8);
	}

	private static String text(ByteSequence value) {
		return value.toString(StandardCharsets.UTF_8);
	}
}
