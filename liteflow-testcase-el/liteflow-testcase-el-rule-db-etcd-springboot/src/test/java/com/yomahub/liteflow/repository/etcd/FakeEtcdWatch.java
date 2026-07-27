package com.yomahub.liteflow.repository.etcd;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * In-memory {@link EtcdWatchFacade} for unit tests. Records every registration
 * (prefix + start revision) and lets tests push events, errors and completions
 * into selected or all active watchers.
 */
final class FakeEtcdWatch implements EtcdWatchFacade {

	private final List<Registration> registrations = new ArrayList<>();

	@Override
	public synchronized Handle watch(String prefix, long startRevision, Listener listener) {
		Registration registration = new Registration(prefix, startRevision, listener);
		registrations.add(registration);
		return () -> {
			synchronized (FakeEtcdWatch.this) {
				registration.closed = true;
			}
		};
	}

	void emitPut(String key, String value, long revision) {
		emit(new Event(key, value, revision, false));
	}

	void emitDelete(String key, long revision) {
		emit(new Event(key, null, revision, true));
	}

	void emit(Event event) {
		for (Registration registration : active()) {
			if (event.key().startsWith(registration.prefix)
					&& (registration.startRevision == 0 || event.revision() >= registration.startRevision)) {
				registration.listener.onEvents(Collections.singletonList(event));
			}
		}
	}

	/** Pushes an error into every active watcher, like a broken grpc stream. */
	void failAll(Throwable error) {
		for (Registration registration : active()) {
			registration.listener.onError(error);
		}
	}

	/** Signals stream completion (mapped to {@code onError(null)} by the jetcd facade). */
	void completeAll() {
		for (Registration registration : active()) {
			registration.listener.onError(null);
		}
	}

	synchronized int registrationCount() {
		return registrations.size();
	}

	synchronized long lastStartRevision(String prefix) {
		long result = -1;
		for (Registration registration : registrations) {
			if (registration.prefix.equals(prefix)) {
				result = registration.startRevision;
			}
		}
		return result;
	}

	synchronized int activeCount() {
		return active().size();
	}

	private synchronized List<Registration> active() {
		List<Registration> result = new ArrayList<>();
		for (Registration registration : registrations) {
			if (!registration.closed) {
				result.add(registration);
			}
		}
		return result;
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
