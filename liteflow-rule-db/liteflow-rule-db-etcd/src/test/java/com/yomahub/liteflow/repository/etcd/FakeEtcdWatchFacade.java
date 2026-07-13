package com.yomahub.liteflow.repository.etcd;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

final class FakeEtcdWatchFacade implements EtcdWatchFacade {

	private final List<Registration> registrations = new ArrayList<>();

	@Override
	public synchronized Handle watch(String prefix, long startRevision, Listener listener) {
		Registration registration = new Registration(prefix, startRevision, listener);
		registrations.add(registration);
		return () -> registration.closed = true;
	}

	synchronized void emitPut(String key, String value, long revision) {
		emit(new Event(key, value, revision, false));
	}

	synchronized void emitDelete(String key, long revision) {
		emit(new Event(key, null, revision, true));
	}

	synchronized void failActive(Throwable error) {
		for (Registration registration : new ArrayList<>(registrations)) {
			if (!registration.closed) {
				registration.listener.onError(error);
				}
			}
		}

		synchronized List<Long> startRevisions() {
		List<Long> result = new ArrayList<>();
		for (Registration registration : registrations) {
			result.add(registration.startRevision);
		}
		return result;
	}

	private void emit(Event event) {
		for (Registration registration : new ArrayList<>(registrations)) {
			if (!registration.closed && event.key().startsWith(registration.prefix)
					&& (registration.startRevision == 0 || event.revision() >= registration.startRevision)) {
				registration.listener.onEvents(Collections.singletonList(event));
			}
		}
	}

	private static final class Registration {
		private final String prefix;
		private final long startRevision;
		private final Listener listener;
		private boolean closed;

		private Registration(String prefix, long startRevision, Listener listener) {
			this.prefix = prefix;
			this.startRevision = startRevision;
			this.listener = listener;
		}
	}
}
