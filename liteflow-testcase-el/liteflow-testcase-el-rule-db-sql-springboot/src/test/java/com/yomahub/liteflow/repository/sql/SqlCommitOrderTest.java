package com.yomahub.liteflow.repository.sql;

import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.PublishResult;
import com.yomahub.liteflow.publisher.RulePublisher;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Verifies that change-log sequence allocation cannot overtake transaction commit order. */
class SqlCommitOrderTest {

	@Test
	void laterPublicationCannotCommitBeforeTheLowerSequence() throws Exception {
		JdbcDataSource delegate = new JdbcDataSource();
		delegate.setURL("jdbc:h2:mem:sql-commit-order;DB_CLOSE_DELAY=-1;MODE=MySQL");
		delegate.setUser("sa");
		try (Connection connection = delegate.getConnection()) {
			new SqlDialect("lf_").createTablesIfAbsent(connection);
		}

		CountDownLatch firstCommitEntered = new CountDownLatch(1);
		CountDownLatch allowFirstCommit = new CountDownLatch(1);
		DataSource dataSource = blockingFirstCommit(delegate, firstCommitEntered, allowFirstCommit);
		ExecutorService executor = Executors.newFixedThreadPool(2);
		try (RulePublisher firstPublisher = publisher(dataSource);
				RulePublisher secondPublisher = publisher(dataSource)) {
			Future<PublishResult> first = executor.submit(() -> firstPublisher.publishChain(chain("first")));
			assertTrue(firstCommitEntered.await(5, TimeUnit.SECONDS), "first publication did not reach commit");

			Future<PublishResult> second = executor.submit(() -> secondPublisher.publishChain(chain("second")));
			assertThrows(TimeoutException.class, () -> second.get(250, TimeUnit.MILLISECONDS),
					"later publication must wait for the transaction holding the publishing-order lock");

			allowFirstCommit.countDown();
			PublishResult firstResult = first.get(5, TimeUnit.SECONDS);
			PublishResult secondResult = second.get(5, TimeUnit.SECONDS);
			assertTrue(firstResult.getSequence() < secondResult.getSequence());
		}
		finally {
			allowFirstCommit.countDown();
			executor.shutdownNow();
		}
	}

	private RulePublisher publisher(DataSource dataSource) {
		return new SqlRulePublisherProvider().create(SqlPublisherConfig.builder()
				.applicationName("commit-order-app").dataSource(dataSource).tablePrefix("lf_").build());
	}

	private PublishChainRequest chain(String id) {
		return PublishChainRequest.builder().chainId(id).el("THEN(a)").expectedVersion(0L).build();
	}

	private DataSource blockingFirstCommit(DataSource delegate, CountDownLatch entered, CountDownLatch release) {
		AtomicInteger borrowed = new AtomicInteger();
		return (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(),
				new Class<?>[] { DataSource.class }, (proxy, method, args) -> {
					Object value = invoke(method, delegate, args);
					if (value instanceof Connection) {
						return blockingCommit((Connection) value, borrowed.incrementAndGet() == 1, entered, release);
					}
					return value;
				});
	}

	private Connection blockingCommit(Connection delegate, boolean block, CountDownLatch entered,
			CountDownLatch release) {
		return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
				new Class<?>[] { Connection.class }, (proxy, method, args) -> {
					if (block && "commit".equals(method.getName())) {
						entered.countDown();
						try {
							if (!release.await(5, TimeUnit.SECONDS)) {
								throw new SQLException("timed out waiting to release the first commit");
							}
						}
						catch (InterruptedException e) {
							Thread.currentThread().interrupt();
							throw new SQLException("interrupted while waiting to commit", e);
						}
					}
					return invoke(method, delegate, args);
				});
	}

	private Object invoke(java.lang.reflect.Method method, Object target, Object[] args) throws Throwable {
		try {
			return method.invoke(target, args);
		}
		catch (InvocationTargetException e) {
			throw e.getCause();
		}
	}
}
