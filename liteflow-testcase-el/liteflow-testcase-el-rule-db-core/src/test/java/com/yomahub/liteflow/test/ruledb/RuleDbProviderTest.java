package com.yomahub.liteflow.test.ruledb;

import com.yomahub.liteflow.repository.ChangeSourceHealth;
import com.yomahub.liteflow.repository.RuleDbProvider;
import com.yomahub.liteflow.repository.RuleDbProviderHolder;
import com.yomahub.liteflow.repository.RuleRepository;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class RuleDbProviderTest {

    private static final String PROVIDER_SERVICE =
            "META-INF/services/com.yomahub.liteflow.repository.RuleDbProvider";

    private final ClassLoader originalContextClassLoader =
            Thread.currentThread().getContextClassLoader();

    @AfterEach
    void reset() {
        RuleDbProviderHolder.reset();
        Thread.currentThread().setContextClassLoader(originalContextClassLoader);
        InMemoryRuleRepository.reset();
        ClosingProvider.CLOSE_COUNT.set(0);
    }

    @Test
    void resolvesProviderAndSharesRepository() {
        RuleDbProvider provider = RuleDbProviderHolder.get();

        assertSame(provider, RuleDbProviderHolder.get());
        assertSame(provider.repository(), RuleDbProviderHolder.repository());
    }

    @Test
    void healthStartsInStartingState() {
        assertEquals(ChangeSourceHealth.Status.STARTING,
                RuleDbProviderHolder.get().changeSource().health().getStatus());
    }

    @Test
    void buffersUntilActivationAndDropsBaselineEvents() {
        RuleDbProvider provider = RuleDbProviderHolder.get();
        List<ChangeRecord> delivered = new ArrayList<>();
        provider.changeSource().open(delivered::addAll);
        ((InMemoryRuleDbProvider) provider).emit(new ChangeRecord(1, ChangeRecord.TargetType.CHAIN,
                "old", ChangeRecord.Op.UPSERT, 1));
        ((InMemoryRuleDbProvider) provider).emit(new ChangeRecord(3, ChangeRecord.TargetType.CHAIN,
                "new", ChangeRecord.Op.UPSERT, 1));

        provider.changeSource().activate(1);

        assertEquals(1, delivered.size());
        assertEquals(3, delivered.get(0).getSeq());
        assertEquals(ChangeSourceHealth.Status.UP,
                provider.changeSource().health().getStatus());
    }

    @Test
    void closeMakesLateEventsNoOp() {
        RuleDbProvider provider = RuleDbProviderHolder.get();
        List<ChangeRecord> delivered = new ArrayList<>();
        provider.changeSource().open(delivered::addAll);
        provider.changeSource().activate(0);
        provider.changeSource().close();
        ((InMemoryRuleDbProvider) provider).emit(new ChangeRecord(1, ChangeRecord.TargetType.CHAIN,
                "late", ChangeRecord.Op.UPSERT, 1));
        provider.close();

        assertTrue(delivered.isEmpty());
        assertEquals(ChangeSourceHealth.Status.DOWN,
                provider.changeSource().health().getStatus());
    }

    @Test
    void liveDeliveryAdvancesHealthCursorAndSuccessTime() throws Exception {
        InMemoryRuleDbProvider provider = (InMemoryRuleDbProvider) RuleDbProviderHolder.get();
        provider.changeSource().open(changes -> { });
        provider.changeSource().activate(5);
        long activatedAt = provider.changeSource().health().getLastSuccessTime();
        waitForClockTick(activatedAt);

        provider.emit(change(6, "live"));

        ChangeSourceHealth health = provider.changeSource().health();
        assertEquals(6, health.getCursor());
        assertTrue(health.getLastSuccessTime() > activatedAt);
    }

    @Test
    void reentrantAndConcurrentEmitsAreSerializedWithoutHoldingMonitor() throws Exception {
        InMemoryRuleDbProvider provider = (InMemoryRuleDbProvider) RuleDbProviderHolder.get();
        List<Long> delivered = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger callbackDepth = new AtomicInteger();
        AtomicInteger maxCallbackDepth = new AtomicInteger();
        CountDownLatch callbackStarted = new CountDownLatch(1);
        CountDownLatch releaseCallback = new CountDownLatch(1);
        provider.changeSource().open(changes -> {
            int depth = callbackDepth.incrementAndGet();
            maxCallbackDepth.accumulateAndGet(depth, Math::max);
            try {
                for (ChangeRecord change : changes) {
                    delivered.add(change.getSeq());
                    if (change.getSeq() == 1) {
                        provider.emit(change(2, "reentrant"));
                        callbackStarted.countDown();
                        await(releaseCallback);
                    }
                }
            } finally {
                callbackDepth.decrementAndGet();
            }
        });
        provider.changeSource().activate(0);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> first = executor.submit(() -> provider.emit(change(1, "first")));
            assertTrue(callbackStarted.await(1, TimeUnit.SECONDS));
            Future<?> concurrent = executor.submit(() -> provider.emit(change(3, "concurrent")));
            concurrent.get(1, TimeUnit.SECONDS);
            releaseCallback.countDown();
            first.get(1, TimeUnit.SECONDS);

            assertEquals(java.util.Arrays.asList(1L, 2L, 3L), delivered);
            assertEquals(1, maxCallbackDepth.get());
        } finally {
            releaseCallback.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void closeDoesNotWaitForBlockedListener() throws Exception {
        InMemoryRuleDbProvider provider = (InMemoryRuleDbProvider) RuleDbProviderHolder.get();
        CountDownLatch callbackStarted = new CountDownLatch(1);
        CountDownLatch releaseCallback = new CountDownLatch(1);
        provider.changeSource().open(changes -> {
            callbackStarted.countDown();
            await(releaseCallback);
        });
        provider.changeSource().activate(0);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> emitter = executor.submit(() -> provider.emit(change(1, "blocked")));
            assertTrue(callbackStarted.await(1, TimeUnit.SECONDS));

            Future<?> closer = executor.submit(() -> provider.changeSource().close());
            closer.get(1, TimeUnit.SECONDS);

            releaseCallback.countDown();
            emitter.get(1, TimeUnit.SECONDS);
            assertEquals(ChangeSourceHealth.Status.DOWN,
                    provider.changeSource().health().getStatus());
        } finally {
            releaseCallback.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void degradedAndDownHealthPreserveLastSuccessAndCursor() {
        ChangeSourceHealth up = ChangeSourceHealth.up(17);
        ChangeSourceHealth degraded = up.degraded("temporary");
        ChangeSourceHealth down = degraded.down("closed");

        assertEquals(ChangeSourceHealth.Status.DEGRADED, degraded.getStatus());
        assertEquals(up.getLastSuccessTime(), degraded.getLastSuccessTime());
        assertEquals(up.getCursor(), degraded.getCursor());
        assertEquals(ChangeSourceHealth.Status.DOWN, down.getStatus());
        assertEquals(up.getLastSuccessTime(), down.getLastSuccessTime());
        assertEquals(up.getCursor(), down.getCursor());
    }

    @Test
    void cachesEmptyResolutionUntilReset() throws Exception {
        IsolatedServicesClassLoader loader = isolatedServices();
        Thread.currentThread().setContextClassLoader(loader);
        RuleDbProviderHolder.reset();

        assertNull(RuleDbProviderHolder.get());
        assertNull(RuleDbProviderHolder.get());
        assertEquals(1, loader.getLookupCount());

        RuleDbProviderHolder.reset();
        assertNull(RuleDbProviderHolder.get());
        assertEquals(2, loader.getLookupCount());
    }

    @Test
    void conflictReportsBothProviderClassNames() throws Exception {
        IsolatedServicesClassLoader loader = isolatedServices(
                FirstProvider.class.getName(), SecondProvider.class.getName());
        Thread.currentThread().setContextClassLoader(loader);
        RuleDbProviderHolder.reset();

        try {
            RuleDbProviderHolder.get();
            fail("expected conflicting providers to fail resolution");
        } catch (com.yomahub.liteflow.exception.ConfigErrorException e) {
            assertTrue(e.getMessage().contains(FirstProvider.class.getName()));
            assertTrue(e.getMessage().contains(SecondProvider.class.getName()));
        }
    }

    @Test
    void resetClosesResolvedProviderExactlyOnce() throws Exception {
        IsolatedServicesClassLoader loader = isolatedServices(ClosingProvider.class.getName());
        Thread.currentThread().setContextClassLoader(loader);
        RuleDbProviderHolder.reset();
        RuleDbProviderHolder.get();

        RuleDbProviderHolder.reset();
        RuleDbProviderHolder.reset();

        assertEquals(1, ClosingProvider.CLOSE_COUNT.get());
    }

    private static ChangeRecord change(long seq, String id) {
        return new ChangeRecord(seq, ChangeRecord.TargetType.CHAIN, id,
                ChangeRecord.Op.UPSERT, 1);
    }

    private static void waitForClockTick(long time) throws InterruptedException {
        while (System.currentTimeMillis() <= time) {
            Thread.sleep(1);
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(1, TimeUnit.SECONDS)) {
                throw new AssertionError("timed out waiting for callback release");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for callback release", e);
        }
    }

    private static IsolatedServicesClassLoader isolatedServices(String... providers) throws IOException {
        List<URL> resources = new ArrayList<>();
        if (providers.length > 0) {
            Path service = Files.createTempDirectory("rule-db-provider-test")
                    .resolve(PROVIDER_SERVICE);
            Files.createDirectories(service.getParent());
            Files.write(service, java.util.Arrays.asList(providers), StandardCharsets.UTF_8);
            service.toFile().deleteOnExit();
            resources.add(service.toUri().toURL());
        }
        return new IsolatedServicesClassLoader(
                RuleDbProviderTest.class.getClassLoader(), resources);
    }

    private static class IsolatedServicesClassLoader extends ClassLoader {
        private final List<URL> providerResources;
        private final AtomicInteger lookupCount = new AtomicInteger();

        IsolatedServicesClassLoader(ClassLoader parent, List<URL> providerResources) {
            super(parent);
            this.providerResources = providerResources;
        }

        @Override
        public Enumeration<URL> getResources(String name) throws IOException {
            if (PROVIDER_SERVICE.equals(name)) {
                lookupCount.incrementAndGet();
                return Collections.enumeration(providerResources);
            }
            return super.getResources(name);
        }

        int getLookupCount() {
            return lookupCount.get();
        }
    }

    public static class FirstProvider extends StubProvider {
    }

    public static class SecondProvider extends StubProvider {
    }

    public static class ClosingProvider extends StubProvider {
        static final AtomicInteger CLOSE_COUNT = new AtomicInteger();

        @Override
        public void close() {
            CLOSE_COUNT.incrementAndGet();
        }
    }

    public static class StubProvider implements RuleDbProvider {
        @Override
        public RuleRepository repository() {
            return new InMemoryRuleRepository();
        }

        @Override
        public com.yomahub.liteflow.repository.RuleChangeSource changeSource() {
            return null;
        }
    }
}
