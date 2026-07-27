package com.yomahub.liteflow.repository.etcd;

import com.yomahub.liteflow.publisher.exception.RuleStorageException;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.Txn;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.common.exception.EtcdExceptionFactory;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.TxnResponse;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchResponse;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Covers the jetcd-backed facades with mocked jetcd clients: value mapping,
 * pinned revisions, error wrapping and watch event translation.
 */
class JetcdFacadeTest {

	@Test
	void getMapsPresentAndAbsentValues() {
		KV kv = mock(KV.class);
		when(kv.get(any(ByteSequence.class)))
				.thenReturn(CompletableFuture.completedFuture(getResponse(9, "k1", "v1", 4L)))
				.thenReturn(CompletableFuture.completedFuture(getResponse(9)));
		JetcdKvFacade facade = new JetcdKvFacade(kv);

		EtcdKvFacade.Value present = facade.get("k1");
		assertTrue(present.exists());
		assertEquals("v1", present.value());
		assertEquals(4, present.modRevision());
		assertEquals(9, present.headerRevision());

		EtcdKvFacade.Value absent = facade.get("k2");
		assertFalse(absent.exists());
		assertNull(absent.value());
	}

	@Test
	void rangeMapsEntriesAndSupportsPinnedRevision() {
		KV kv = mock(KV.class);
		when(kv.get(any(ByteSequence.class), any(GetOption.class)))
				.thenReturn(CompletableFuture.completedFuture(getResponse(7, "p/a", "1", 3L, "p/b", "2", 4L)));
		JetcdKvFacade facade = new JetcdKvFacade(kv);

		EtcdKvFacade.Range range = facade.range("p/");
		assertEquals(7, range.revision());
		assertEquals(2, range.entries().size());
		assertEquals("p/a", range.entries().get(0).key());
		assertEquals("1", range.entries().get(0).value());
		assertEquals(4, range.entries().get(1).modRevision());

		EtcdKvFacade.Range pinned = facade.range("p/", 5);
		assertEquals(2, pinned.entries().size());
	}

	@Test
	void transactionsMapSuccessAndFailure() throws Exception {
		KV kv = mock(KV.class);
		Txn txn = mock(Txn.class);
		when(kv.txn()).thenReturn(txn);
		when(txn.If(any())).thenReturn(txn);
		when(txn.Then(any())).thenReturn(txn);
		when(txn.commit())
				.thenReturn(CompletableFuture.completedFuture(txnResponse(true, 11)))
				.thenReturn(CompletableFuture.completedFuture(txnResponse(false, 11)))
				.thenReturn(CompletableFuture.completedFuture(txnResponse(true, 12)))
				.thenReturn(CompletableFuture.completedFuture(txnResponse(false, 12)));
		JetcdKvFacade facade = new JetcdKvFacade(kv);

		assertTrue(facade.putPair("m", 0, "c", "content", "meta").succeeded());
		assertFalse(facade.putPair("m", 0, "c", "content", "meta").succeeded());
		EtcdKvFacade.TxnResult deleted = facade.deletePair("m", 3, "c");
		assertTrue(deleted.succeeded());
		assertEquals(12, deleted.revision());
		assertFalse(facade.deletePair("m", 3, "c").succeeded());
	}

	@Test
	void storageFailuresAreWrappedWithCause() {
		KV kv = mock(KV.class);
		CompletableFuture<GetResponse> failed = new CompletableFuture<>();
		failed.completeExceptionally(new RuntimeException("unavailable"));
		when(kv.get(any(ByteSequence.class))).thenReturn(failed);
		JetcdKvFacade facade = new JetcdKvFacade(kv);

		RuleStorageException failure = assertThrows(RuleStorageException.class, () -> facade.get("k"));
		assertTrue(failure.getMessage().contains("unavailable"));
		assertTrue(failure.getCause() instanceof RuntimeException);
	}

	@Test
	void interruptionRestoresFlagAndFailsFast() throws Exception {
		KV kv = mock(KV.class);
		@SuppressWarnings("unchecked")
		CompletableFuture<GetResponse> interrupted = mock(CompletableFuture.class);
		when(interrupted.get()).thenThrow(new InterruptedException("stop"));
		when(kv.get(any(ByteSequence.class))).thenReturn(interrupted);
		JetcdKvFacade facade = new JetcdKvFacade(kv);

		try {
			assertThrows(RuleStorageException.class, () -> facade.get("k"));
			assertTrue(Thread.currentThread().isInterrupted());
		}
		finally {
			Thread.interrupted();
		}
	}

	@Test
	void watchTranslatesEventsAndClosesWatcher() {
		Watch watch = mock(Watch.class);
		Watch.Watcher watcher = mock(Watch.Watcher.class);
		AtomicReference<Watch.Listener> captured = new AtomicReference<>();
		when(watch.watch(any(ByteSequence.class), any(WatchOption.class), any(Watch.Listener.class)))
				.thenAnswer(invocation -> {
					captured.set(invocation.getArgument(2));
					return watcher;
				});

		List<EtcdWatchFacade.Event> events = new ArrayList<>();
		List<Throwable> errors = new ArrayList<>();
		JetcdWatchFacade facade = new JetcdWatchFacade(watch);
		EtcdWatchFacade.Handle handle = facade.watch("p/", 9, new EtcdWatchFacade.Listener() {
			@Override
			public void onEvents(List<EtcdWatchFacade.Event> batch) {
				events.addAll(batch);
			}

			@Override
			public void onError(Throwable error) {
				errors.add(error);
			}
		});

		captured.get().onNext(watchResponse(
				event(io.etcd.jetcd.api.Event.EventType.PUT, "p/a", "v1", 7),
				event(io.etcd.jetcd.api.Event.EventType.DELETE, "p/b", null, 8)));
		assertEquals(2, events.size());
		assertEquals("p/a", events.get(0).key());
		assertEquals("v1", events.get(0).value());
		assertEquals(7, events.get(0).revision());
		assertFalse(events.get(0).delete());
		assertTrue(events.get(1).delete());
		assertNull(events.get(1).value());

		handle.close();
		verify(watcher).close();
	}

	@Test
	void watchClassifiesCompactionByTypeNotMessage() {
		Watch watch = mock(Watch.class);
		AtomicReference<Watch.Listener> captured = new AtomicReference<>();
		when(watch.watch(any(ByteSequence.class), any(WatchOption.class), any(Watch.Listener.class)))
				.thenAnswer(invocation -> {
					captured.set(invocation.getArgument(2));
					return mock(Watch.Watcher.class);
				});

		List<Throwable> errors = new ArrayList<>();
		JetcdWatchFacade facade = new JetcdWatchFacade(watch);
		facade.watch("p/", 0, new EtcdWatchFacade.Listener() {
			@Override
			public void onEvents(List<EtcdWatchFacade.Event> batch) {
			}

			@Override
			public void onError(Throwable error) {
				errors.add(error);
			}
		});

		captured.get().onError(EtcdExceptionFactory.newCompactedException(5));
		assertTrue(errors.get(0) instanceof EtcdCompactionException);

		RuntimeException plain = new RuntimeException("stream reset");
		captured.get().onError(plain);
		assertSame(plain, errors.get(1));

		captured.get().onCompleted();
		assertNull(errors.get(2));
	}

	private static GetResponse getResponse(long revision, Object... kvs) {
		io.etcd.jetcd.api.RangeResponse.Builder builder = io.etcd.jetcd.api.RangeResponse.newBuilder()
				.setHeader(io.etcd.jetcd.api.ResponseHeader.newBuilder().setRevision(revision));
		for (int i = 0; i < kvs.length; i += 3) {
			builder.addKvs(io.etcd.jetcd.api.KeyValue.newBuilder()
					.setKey(com.google.protobuf.ByteString.copyFromUtf8((String) kvs[i]))
					.setValue(com.google.protobuf.ByteString.copyFromUtf8((String) kvs[i + 1]))
					.setModRevision((Long) kvs[i + 2]));
		}
		return new GetResponse(builder.build(), ByteSequence.from("".getBytes(StandardCharsets.UTF_8)));
	}

	private static TxnResponse txnResponse(boolean succeeded, long revision) {
		return new TxnResponse(io.etcd.jetcd.api.TxnResponse.newBuilder()
				.setHeader(io.etcd.jetcd.api.ResponseHeader.newBuilder().setRevision(revision))
				.setSucceeded(succeeded)
				.build(), ByteSequence.from("".getBytes(StandardCharsets.UTF_8)));
	}

	private static io.etcd.jetcd.api.Event event(io.etcd.jetcd.api.Event.EventType type,
			String key, String value, long modRevision) {
		io.etcd.jetcd.api.KeyValue.Builder kv = io.etcd.jetcd.api.KeyValue.newBuilder()
				.setKey(com.google.protobuf.ByteString.copyFromUtf8(key))
				.setModRevision(modRevision);
		if (value != null) {
			kv.setValue(com.google.protobuf.ByteString.copyFromUtf8(value));
		}
		return io.etcd.jetcd.api.Event.newBuilder().setType(type).setKv(kv).build();
	}

	private static WatchResponse watchResponse(io.etcd.jetcd.api.Event... events) {
		io.etcd.jetcd.api.WatchResponse.Builder builder = io.etcd.jetcd.api.WatchResponse.newBuilder()
				.setHeader(io.etcd.jetcd.api.ResponseHeader.newBuilder().setRevision(1));
		for (io.etcd.jetcd.api.Event event : events) {
			builder.addEvents(event);
		}
		return new WatchResponse(builder.build());
	}
}
