# Rule-DB Core, Publisher, SQL, and Redis Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the unified Rule-DB runtime and Publisher API, then make SQL and Redis share polling plus reconciliation with no Redis pub/sub.

**Architecture:** Core loads one `RuleDbProvider`, initializes a two-phase `RuleChangeSource`, and tracks desired versus active versions so failed refreshes keep the last successful executable. The Publisher module exposes backend-neutral operations while SQL and Redis modules supply typed configuration, repository, change-source, and publisher providers.

**Tech Stack:** Java 8-compatible source, Maven, JUnit 5, ServiceLoader, Caffeine, JDBC/H2, Redisson, Redis Lua, Groovy script executor.

---

**Prerequisite:** Read `docs/superpowers/specs/2026-07-12-rule-db-unified-backends-design.md`. Preserve the existing uncommitted edits in `RuleDbCache.java`, `RuleDbSqlTest.java`, and `docs/liteflow-rule-db-guide.md`; incorporate rather than overwrite them.

## File Map

- Core SPI: `liteflow-core/src/main/java/com/yomahub/liteflow/repository/{RuleDbProvider,RuleChangeSource,ChangeSourceHealth,RuleDbProviderHolder}.java`
- Core runtime: `RuleDbRuntime.java`, `RuleDbSyncManager.java`, `RuleDbCache.java`, plus focused state classes under `repository/runtime/`
- Publisher API: new `liteflow-rule-db/liteflow-rule-db-publisher/`
- SQL adapters: `liteflow-rule-db-sql/.../repository/sql/`
- Redis adapters: `liteflow-rule-db-redis/.../repository/redis/` and `src/main/resources/lua/`
- Contract fixture: `liteflow-testcase-el-rule-db-core`
- Integration tests: existing SQL and Redis Spring Boot Rule-DB testcase modules

### Task 1: Introduce Provider and ChangeSource SPIs

**Files:**
- Create: `liteflow-core/src/main/java/com/yomahub/liteflow/repository/RuleDbProvider.java`
- Create: `liteflow-core/src/main/java/com/yomahub/liteflow/repository/RuleChangeSource.java`
- Create: `liteflow-core/src/main/java/com/yomahub/liteflow/repository/ChangeSourceHealth.java`
- Create: `liteflow-core/src/main/java/com/yomahub/liteflow/repository/RuleDbProviderHolder.java`
- Modify: `liteflow-core/src/main/java/com/yomahub/liteflow/repository/RuleChangeListener.java`
- Modify: `liteflow-core/src/main/java/com/yomahub/liteflow/repository/RuleRepository.java`
- Create: `liteflow-testcase-el/liteflow-testcase-el-rule-db-core/src/test/java/com/yomahub/liteflow/test/ruledb/InMemoryRuleDbProvider.java`
- Replace: `liteflow-testcase-el/liteflow-testcase-el-rule-db-core/src/test/resources/META-INF/services/com.yomahub.liteflow.repository.RuleRepository`
- Create: `liteflow-testcase-el/liteflow-testcase-el-rule-db-core/src/test/resources/META-INF/services/com.yomahub.liteflow.repository.RuleDbProvider`
- Test: `liteflow-testcase-el/liteflow-testcase-el-rule-db-core/src/test/java/com/yomahub/liteflow/test/ruledb/RuleDbProviderTest.java`

- [ ] **Step 1: Write the failing provider-resolution tests**

```java
@Test
void resolvesProviderAndSharesRepository() {
    RuleDbProvider provider = RuleDbProviderHolder.get();
    assertNotNull(provider);
    assertSame(provider.repository(), RuleDbProviderHolder.repository());
}

@Test
void healthStartsInStartingState() {
    assertEquals(ChangeSourceHealth.Status.STARTING,
            RuleDbProviderHolder.get().changeSource().health().getStatus());
}
```

- [ ] **Step 2: Run the test and verify the new types are missing**

Run: `mvn test -pl liteflow-testcase-el/liteflow-testcase-el-rule-db-core -Dtest=RuleDbProviderTest`

Expected: compilation fails because `RuleDbProvider` and `RuleDbProviderHolder` do not exist.

- [ ] **Step 3: Add the minimal SPI contracts and holder**

```java
public interface RuleDbProvider extends AutoCloseable {
    RuleRepository repository();
    RuleChangeSource changeSource();
    @Override default void close() { }
}

public interface RuleChangeSource extends AutoCloseable {
    void open(RuleChangeListener listener);
    void activate(long baselineSeq);
    ChangeSourceHealth health();
    @Override default void close() { }
}

@FunctionalInterface
public interface RuleChangeListener {
    void onChanges(List<ChangeRecord> changes);
    default void onReconcileRequired() { }
}
```

`RuleDbProviderHolder` must resolve exactly one provider with `ServiceLoader`, cache it, expose `repository()`, list conflicting implementation class names in `ConfigErrorException`, and reset both provider and resolution state for tests. ChangeSource implementations call `onReconcileRequired()` for a polling gap, compacted watch revision, or session rebuild; the default keeps the listener usable as a lambda.

Keep `fetchLatestSeq`, `fetchChangesSince`, `subscribe`, and `defaultSeqPollSeconds` temporarily as deprecated defaults on `RuleRepository`; Tasks 8 and 9 remove all production use before Task 10 deletes them.

- [ ] **Step 4: Convert the in-memory fixture to a provider**

`InMemoryRuleDbProvider` returns the existing repository and a controllable buffered source. Its source records calls to `open` and `activate`, buffers `emit(ChangeRecord)` before activation, drops events at or below the baseline, and reports health.

- [ ] **Step 5: Run the focused test**

Run: `mvn test -pl liteflow-testcase-el/liteflow-testcase-el-rule-db-core -Dtest=RuleDbProviderTest`

Expected: all provider tests pass.

- [ ] **Step 6: Commit**

```bash
git add liteflow-core/src/main/java/com/yomahub/liteflow/repository \
  liteflow-testcase-el/liteflow-testcase-el-rule-db-core/src/test
git commit -m "feat(core): add rule-db provider and change source SPIs"
```

### Task 2: Implement Two-Phase Synchronization

**Files:**
- Modify: `liteflow-core/src/main/java/com/yomahub/liteflow/repository/RuleDbSyncManager.java`
- Modify: `liteflow-core/src/main/java/com/yomahub/liteflow/repository/RuleDbRuntime.java`
- Modify: `liteflow-testcase-el/liteflow-testcase-el-rule-db-core/src/test/java/com/yomahub/liteflow/test/ruledb/InMemoryRuleDbProvider.java`
- Test: `liteflow-testcase-el/liteflow-testcase-el-rule-db-core/src/test/java/com/yomahub/liteflow/test/ruledb/RuleDbChangeSourceTest.java`

- [ ] **Step 1: Add failing initialization-window tests**

```java
@Test
void replaysEventArrivingBetweenOpenAndManifest() {
    InMemoryRuleDbProvider.emitDuringManifest(
            new ChangeRecord(2, TargetType.CHAIN, "chain1", Op.UPSERT, 2));
    InMemoryRuleRepository.putChainVersion("chain1", "THEN(b, a)", 2);
    FlowExecutor executor = buildExecutor(new RuleDbConfig());
    assertEquals("b==>a", executor.execute2Resp("chain1", "arg").getExecuteStepStr());
}

@Test
void closeMakesLateCallbackNoOp() {
    buildExecutor(new RuleDbConfig());
    RuleDbRuntime.destroy();
    InMemoryRuleDbProvider.emit(new ChangeRecord(3, TargetType.CHAIN, "late", Op.UPSERT, 1));
    assertFalse(FlowBus.containChain("late"));
}
```

- [ ] **Step 2: Run and verify failure**

Run: `mvn test -pl liteflow-testcase-el/liteflow-testcase-el-rule-db-core -Dtest=RuleDbChangeSourceTest`

Expected: tests fail because runtime fetches manifest before opening the change source and late callbacks remain active.

- [ ] **Step 3: Replace backend branching with the two-phase flow**

Implement this order in `RuleDbRuntime.init()`:

```java
RuleDbProvider provider = RuleDbProviderHolder.get();
RuleDbSyncManager.open(provider);
RuleManifest manifest = provider.repository().fetchManifest();
initializeShadows(manifest);
RuleDbSyncManager.activate(manifest.getLatestSeq());
RuleDbSyncManager.startReconcileScheduler();
```

`RuleDbSyncManager` owns a `running` guard, applies buffered batches in ascending seq order, advances the cursor monotonically, and leaves polling scheduling inside SQL/Redis ChangeSource implementations. Keep public `pollOnce()` only as a test hook that delegates when the source implements package-private `ManualPollingChangeSource`.

- [ ] **Step 4: Run all core Rule-DB tests**

Run: `mvn test -pl liteflow-testcase-el/liteflow-testcase-el-rule-db-core`

Expected: all existing tests plus `RuleDbChangeSourceTest` pass.

- [ ] **Step 5: Commit**

```bash
git add liteflow-core/src/main/java/com/yomahub/liteflow/repository \
  liteflow-testcase-el/liteflow-testcase-el-rule-db-core/src/test
git commit -m "refactor(core): unify rule-db change synchronization"
```

### Task 3: Add Metadata Lookups and Desired/Active State

**Files:**
- Create: `liteflow-core/src/main/java/com/yomahub/liteflow/repository/runtime/RuleTargetState.java`
- Create: `liteflow-core/src/main/java/com/yomahub/liteflow/repository/runtime/RuleTargetStatus.java`
- Modify: `liteflow-core/src/main/java/com/yomahub/liteflow/repository/RuleRepository.java`
- Modify: `liteflow-core/src/main/java/com/yomahub/liteflow/repository/RuleDbRuntime.java`
- Modify: `liteflow-testcase-el/liteflow-testcase-el-rule-db-core/src/test/java/com/yomahub/liteflow/test/ruledb/InMemoryRuleRepository.java`
- Test: `liteflow-testcase-el/liteflow-testcase-el-rule-db-core/src/test/java/com/yomahub/liteflow/test/ruledb/RuleDbVersionStateTest.java`

- [ ] **Step 1: Add failing state and metadata-only tests**

```java
@Test
void newScriptEventFetchesOnlyMetadata() {
    buildExecutor(new RuleDbConfig());
    InMemoryRuleRepository.publishScript("s9", "println('x')", "script", "groovy");
    InMemoryRuleDbProvider.emitLastChange();
    assertEquals(1, InMemoryRuleRepository.FETCH_SCRIPT_META_COUNT.get());
    assertEquals(0, InMemoryRuleRepository.FETCH_SCRIPT_COUNT.get());
}

@Test
void upsertChangesDesiredButKeepsActive() {
    loadAndExecuteVersionOne();
    applyVersionTwoChange();
    RuleTargetState state = RuleDbRuntime.chainState("chain1");
    assertEquals(2, state.getDesiredVersion());
    assertEquals(1, state.getActiveVersion());
    assertEquals(RuleTargetStatus.STALE, state.getStatus());
}
```

- [ ] **Step 2: Run and verify failure**

Run: `mvn test -pl liteflow-testcase-el/liteflow-testcase-el-rule-db-core -Dtest=RuleDbVersionStateTest`

Expected: compilation fails because metadata lookup methods and state classes do not exist.

- [ ] **Step 3: Add focused state objects and metadata methods**

```java
public enum RuleTargetStatus { SHADOW, READY, STALE, LOADING, FAILED, DELETED }

public final class RuleTargetState {
    private final AtomicLong desiredVersion = new AtomicLong();
    private final AtomicLong activeVersion = new AtomicLong();
    private final ReentrantLock loadLock = new ReentrantLock();
    private volatile String desiredMd5;
    private volatile String activeMd5;
    private volatile RuleTargetStatus status = RuleTargetStatus.SHADOW;
    private volatile Throwable lastError;
}
```

Add `fetchChainMeta` and `fetchScriptMeta` to `RuleRepository`. Convert the version and md5 maps in `RuleDbRuntime` to `ConcurrentHashMap<String, RuleTargetState>`. UPSERT updates desired fields only; DELETE marks `DELETED` and removes the FlowBus shadow. Reconciliation compares manifest metadata with desired fields, not active fields.

- [ ] **Step 4: Run the focused and existing convergence tests**

Run: `mvn test -pl liteflow-testcase-el/liteflow-testcase-el-rule-db-core -Dtest=RuleDbVersionStateTest,RuleDbConvergeTest,RuleDbDeleteTest`

Expected: all selected tests pass.

- [ ] **Step 5: Commit**

```bash
git add liteflow-core/src/main/java/com/yomahub/liteflow/repository \
  liteflow-testcase-el/liteflow-testcase-el-rule-db-core/src/test
git commit -m "refactor(core): track desired and active rule versions"
```

### Task 4: Keep the Last Successful Chain Version

**Files:**
- Create: `liteflow-core/src/main/java/com/yomahub/liteflow/repository/runtime/ChainCandidateLoader.java`
- Modify: `liteflow-core/src/main/java/com/yomahub/liteflow/repository/RuleDbRuntime.java`
- Modify: `liteflow-core/src/main/java/com/yomahub/liteflow/flow/element/Chain.java`
- Modify: `liteflow-core/src/main/java/com/yomahub/liteflow/builder/el/LiteFlowChainELBuilder.java`
- Test: `liteflow-testcase-el/liteflow-testcase-el-rule-db-core/src/test/java/com/yomahub/liteflow/test/ruledb/RuleDbLastGoodChainTest.java`

- [ ] **Step 1: Add failing invalid-update and single-flight tests**

```java
@Test
void invalidNewElKeepsVersionOneExecutable() {
    FlowExecutor executor = loadVersionOne("THEN(a, b)");
    InMemoryRuleRepository.publishChain("chain1", "THEN(a, missing)");
    InMemoryRuleDbProvider.emitLastChange();
    assertEquals("a==>b", executor.execute2Resp("chain1", "arg").getExecuteStepStr());
    assertEquals(RuleTargetStatus.FAILED, RuleDbRuntime.chainState("chain1").getStatus());
}

@Test
void concurrentRefreshFetchesCandidateOnce() throws Exception {
    FlowExecutor executor = loadVersionOne("THEN(a, b)");
    publishVersionTwo("THEN(b, a)");
    executeConcurrently(executor, "chain1", 20);
    assertEquals(2, InMemoryRuleRepository.FETCH_CHAIN_COUNT.get());
}
```

- [ ] **Step 2: Run and verify the old graph is destroyed**

Run: `mvn test -pl liteflow-testcase-el/liteflow-testcase-el-rule-db-core -Dtest=RuleDbLastGoodChainTest`

Expected: invalid update fails execution because current invalidation clears the active EL and condition tree.

- [ ] **Step 3: Build and atomically install a candidate Chain**

`ChainCandidateLoader` must fetch metadata, fetch content, fetch metadata again, reject a version change during the read, build a temporary Chain with a collision-safe internal ID, and return its compiled condition list plus EL metadata. Do not insert the temporary Chain into the public `FlowBus` map after compilation.

Add one synchronized installation method to `Chain` that swaps `el`, `elMd5`, `routeEl`, namespace, condition list, and compiled flag together. On failure, set state to FAILED and retain the previous fields. On a first load with no active version, rethrow the load/compile exception.

- [ ] **Step 4: Run chain runtime tests**

Run: `mvn test -pl liteflow-testcase-el/liteflow-testcase-el-rule-db-core -Dtest=RuleDbLastGoodChainTest,RuleDbLazyLoadTest,RuleDbEvictionTest,RuleDbConvergeTest`

Expected: all selected tests pass.

- [ ] **Step 5: Commit**

```bash
git add liteflow-core/src/main/java/com/yomahub/liteflow/repository \
  liteflow-core/src/main/java/com/yomahub/liteflow/flow/element/Chain.java \
  liteflow-core/src/main/java/com/yomahub/liteflow/builder/el/LiteFlowChainELBuilder.java \
  liteflow-testcase-el/liteflow-testcase-el-rule-db-core/src/test
git commit -m "feat(core): retain last successful rule-db chain"
```

### Task 5: Keep the Last Successful Script Version

**Files:**
- Create: `liteflow-core/src/main/java/com/yomahub/liteflow/repository/runtime/ScriptCandidateLoader.java`
- Modify: `liteflow-core/src/main/java/com/yomahub/liteflow/repository/RuleDbRuntime.java`
- Modify: `liteflow-core/src/main/java/com/yomahub/liteflow/flow/FlowBus.java`
- Modify: `liteflow-core/src/main/java/com/yomahub/liteflow/repository/RuleDbCache.java`
- Test: `liteflow-testcase-el/liteflow-testcase-el-rule-db-core/src/test/java/com/yomahub/liteflow/test/ruledb/RuleDbLastGoodScriptTest.java`

- [ ] **Step 1: Add failing script rollback tests**

```java
@Test
void invalidScriptKeepsOldCompiledArtifact() {
    FlowExecutor executor = loadChainWithScript("defaultContext.setData(\"v\", \"old\");");
    publishScript("this is not valid groovy !");
    InMemoryRuleDbProvider.emitLastChange();
    LiteflowResponse response = executor.execute2Resp("chain1", "arg");
    assertEquals("old", response.getContextBean(DefaultContext.class).getData("v"));
    assertEquals(RuleTargetStatus.FAILED, RuleDbRuntime.scriptState("s1").getStatus());
}
```

- [ ] **Step 2: Run and verify failure**

Run: `mvn test -pl liteflow-testcase-el/liteflow-testcase-el-rule-db-core -Dtest=RuleDbLastGoodScriptTest`

Expected: execution fails because current invalidation unloads the old executor artifact before compiling the new source.

- [ ] **Step 3: Validate a versioned candidate before activation**

Compile the candidate with an internal ID `nodeId + "@ruleDb@" + version`. Only after candidate compilation succeeds, load the same source under the public node ID, update active state, mark referencing cached chains STALE, and unload the candidate ID. If public activation fails, reload the saved active source before surfacing FAILED. Extend cache bookkeeping so eviction unloads only artifacts that no active chain references.

- [ ] **Step 4: Run script and cache tests**

Run: `mvn test -pl liteflow-testcase-el/liteflow-testcase-el-rule-db-core -Dtest=RuleDbLastGoodScriptTest,RuleDbScriptTypeTest,RuleDbEvictionTest,RuleDbConvergeTest`

Expected: all selected tests pass, including the existing chain-only script reference regression.

- [ ] **Step 5: Commit**

```bash
git add liteflow-core/src/main/java/com/yomahub/liteflow/repository \
  liteflow-core/src/main/java/com/yomahub/liteflow/flow/FlowBus.java \
  liteflow-testcase-el/liteflow-testcase-el-rule-db-core/src/test
git commit -m "feat(core): retain last successful rule-db script"
```

### Task 6: Create the Unified Publisher Module

**Files:**
- Modify: `liteflow-rule-db/pom.xml`
- Create: `liteflow-rule-db/liteflow-rule-db-publisher/pom.xml`
- Create: `liteflow-rule-db/liteflow-rule-db-publisher/src/main/java/com/yomahub/liteflow/publisher/RulePublisher.java`
- Create: `liteflow-rule-db/liteflow-rule-db-publisher/src/main/java/com/yomahub/liteflow/publisher/RulePublisherConfig.java`
- Create: `liteflow-rule-db/liteflow-rule-db-publisher/src/main/java/com/yomahub/liteflow/publisher/RulePublisherProvider.java`
- Create: `liteflow-rule-db/liteflow-rule-db-publisher/src/main/java/com/yomahub/liteflow/publisher/RulePublisherFactory.java`
- Create: `liteflow-rule-db/liteflow-rule-db-publisher/src/main/java/com/yomahub/liteflow/publisher/PublisherBackend.java`
- Create: `liteflow-rule-db/liteflow-rule-db-publisher/src/main/java/com/yomahub/liteflow/publisher/PublishChainRequest.java`
- Create: `liteflow-rule-db/liteflow-rule-db-publisher/src/main/java/com/yomahub/liteflow/publisher/PublishScriptRequest.java`
- Create: `liteflow-rule-db/liteflow-rule-db-publisher/src/main/java/com/yomahub/liteflow/publisher/RemoveRuleRequest.java`
- Create: `liteflow-rule-db/liteflow-rule-db-publisher/src/main/java/com/yomahub/liteflow/publisher/PublishResult.java`
- Create: `liteflow-rule-db/liteflow-rule-db-publisher/src/main/java/com/yomahub/liteflow/publisher/exception/PublisherConfigurationException.java`
- Create: `liteflow-rule-db/liteflow-rule-db-publisher/src/main/java/com/yomahub/liteflow/publisher/exception/PublisherProviderNotFoundException.java`
- Create: `liteflow-rule-db/liteflow-rule-db-publisher/src/main/java/com/yomahub/liteflow/publisher/exception/VersionConflictException.java`
- Create: `liteflow-rule-db/liteflow-rule-db-publisher/src/main/java/com/yomahub/liteflow/publisher/exception/RuleValidationException.java`
- Create: `liteflow-rule-db/liteflow-rule-db-publisher/src/main/java/com/yomahub/liteflow/publisher/exception/RuleStorageException.java`
- Test: `liteflow-rule-db/liteflow-rule-db-publisher/src/test/java/com/yomahub/liteflow/publisher/RulePublisherFactoryTest.java`

- [ ] **Step 1: Add the module and failing Factory tests**

```java
@Test
void rejectsMissingProvider() {
    assertThrows(PublisherProviderNotFoundException.class,
            () -> RulePublisherFactory.create(new MissingTestConfig("app")));
}

@Test
void expectedVersionSemanticsAreRepresentable() {
    assertNull(PublishChainRequest.builder().chainId("c").el("THEN(a)").build().getExpectedVersion());
    assertEquals(0L, PublishChainRequest.builder().chainId("c").el("THEN(a)")
            .expectedVersion(0L).build().getExpectedVersion());
}
```

- [ ] **Step 2: Run and verify failure**

Run: `mvn test -pl liteflow-rule-db/liteflow-rule-db-publisher -am`

Expected: Maven fails because the module and API do not exist.

- [ ] **Step 3: Implement the public API**

```java
public interface RulePublisher extends AutoCloseable {
    PublishResult publishChain(PublishChainRequest request);
    PublishResult publishScript(PublishScriptRequest request);
    PublishResult removeChain(RemoveRuleRequest request);
    PublishResult removeScript(RemoveRuleRequest request);
}

public interface RulePublisherProvider {
    boolean supports(RulePublisherConfig config);
    RulePublisher create(RulePublisherConfig config);
}
```

Use immutable request/result objects with builders. Validate nonblank IDs and required content before provider invocation. Define the exact exception hierarchy from the design. Factory selects exactly one supporting provider through ServiceLoader and never reads `LiteflowConfigGetter`.

- [ ] **Step 4: Run Publisher tests**

Run: `mvn test -pl liteflow-rule-db/liteflow-rule-db-publisher -am`

Expected: Publisher module and dependencies build; all Factory tests pass.

- [ ] **Step 5: Commit**

```bash
git add liteflow-rule-db/pom.xml liteflow-rule-db/liteflow-rule-db-publisher
git commit -m "feat(rule-db): add unified publisher API"
```

### Task 7: Introduce Grouped Execution Configuration

**Files:**
- Modify: `liteflow-core/src/main/java/com/yomahub/liteflow/property/RuleDbConfig.java`
- Create: `liteflow-core/src/main/java/com/yomahub/liteflow/property/RuleDbCacheConfig.java`
- Create: `liteflow-core/src/main/java/com/yomahub/liteflow/property/RuleDbSyncConfig.java`
- Create: `liteflow-core/src/main/java/com/yomahub/liteflow/property/RuleDbSqlConfig.java`
- Create: `liteflow-core/src/main/java/com/yomahub/liteflow/property/RuleDbRedisConfig.java`
- Create: `liteflow-core/src/main/java/com/yomahub/liteflow/property/RuleDbEtcdConfig.java`
- Create: `liteflow-core/src/main/java/com/yomahub/liteflow/property/RuleDbZkConfig.java`
- Modify: `liteflow-spring-boot-starter/src/main/java/com/yomahub/liteflow/springboot/LiteflowProperty.java`
- Modify: `liteflow-spring-boot4-starter/src/main/java/com/yomahub/liteflow/springboot4/LiteflowProperty.java`
- Modify: `liteflow-solon-plugin/src/main/java/com/yomahub/liteflow/solon/config/LiteflowProperty.java`
- Test: `liteflow-testcase-el/liteflow-testcase-el-rule-db-core/src/test/java/com/yomahub/liteflow/test/ruledb/RuleDbConfigTest.java`
- Create: `liteflow-testcase-el/liteflow-testcase-el-springboot/src/test/java/com/yomahub/liteflow/test/config/RuleDbConfigBindingTest.java`
- Create: `liteflow-testcase-el/liteflow-testcase-el-springboot4/src/test/java/com/yomahub/liteflow/test/config/RuleDbConfigBindingTest.java`
- Create: `liteflow-testcase-el/liteflow-testcase-el-solon/src/test/java/com/yomahub/liteflow/test/config/RuleDbConfigBindingTest.java`

- [ ] **Step 1: Add failing default and nested-value tests**

```java
@Test
void exposesGroupedDefaults() {
    RuleDbConfig config = new RuleDbConfig();
    assertEquals(500, config.getCache().getCapacity());
    assertEquals(60, config.getSync().getReconcileSeconds());
    assertEquals(3, config.getSync().getFetchRetryTimes());
    assertNull(config.getSync().getPollSeconds());
}
```

Framework tests bind `liteflow.rule-db.cache.capacity=123`, `sync.poll-seconds=5`, SQL table prefix, Redis key prefix, etcd root path, and ZK root path, then assert the final `LiteflowConfig.getRuleDb()` nested values.

- [ ] **Step 2: Run and verify failure**

Run:

```bash
mvn test -pl liteflow-testcase-el/liteflow-testcase-el-rule-db-core,\
liteflow-testcase-el/liteflow-testcase-el-springboot,\
liteflow-testcase-el/liteflow-testcase-el-springboot4,\
liteflow-testcase-el/liteflow-testcase-el-solon \
  -Dtest='RuleDbConfigTest,RuleDbConfigBindingTest' -Dsurefire.failIfNoSpecifiedTests=false
```

Expected: test compilation fails because grouped configuration classes do not exist.

- [ ] **Step 3: Replace the flat field union**

```java
public class RuleDbConfig {
    private Boolean enabled = Boolean.TRUE;
    private String applicationName;
    private RuleDbCacheConfig cache = new RuleDbCacheConfig();
    private RuleDbSyncConfig sync = new RuleDbSyncConfig();
    private RuleDbSqlConfig sql = new RuleDbSqlConfig();
    private RuleDbRedisConfig redis = new RuleDbRedisConfig();
    private RuleDbEtcdConfig etcd = new RuleDbEtcdConfig();
    private RuleDbZkConfig zk = new RuleDbZkConfig();
}
```

Move every existing SQL/Redis property without flat aliases. Add etcd endpoints/user/password/root path and ZK connect string/session timeout/root path. Update core runtime reads to `cache` and `sync`; SQL/Redis adapters in subsequent tasks use their grouped sections. Framework `LiteflowProperty` classes continue holding one nested `RuleDbConfig`.

- [ ] **Step 4: Run core and framework configuration tests**

Run:

```bash
mvn test -pl liteflow-testcase-el/liteflow-testcase-el-rule-db-core,\
liteflow-testcase-el/liteflow-testcase-el-springboot,\
liteflow-testcase-el/liteflow-testcase-el-springboot4,\
liteflow-testcase-el/liteflow-testcase-el-solon \
  -Dtest='RuleDbConfigTest,RuleDbConfigBindingTest' -Dsurefire.failIfNoSpecifiedTests=false
```

Expected: BUILD SUCCESS.

- [ ] **Step 5: Commit**

```bash
git add liteflow-core/src/main/java/com/yomahub/liteflow/property \
  liteflow-spring-boot-starter/src/main/java/com/yomahub/liteflow/springboot/LiteflowProperty.java \
  liteflow-spring-boot4-starter/src/main/java/com/yomahub/liteflow/springboot4/LiteflowProperty.java \
  liteflow-solon-plugin/src/main/java/com/yomahub/liteflow/solon/config/LiteflowProperty.java \
  liteflow-testcase-el/liteflow-testcase-el-rule-db-core/src/test \
  liteflow-testcase-el/liteflow-testcase-el-springboot/src/test/java/com/yomahub/liteflow/test/config \
  liteflow-testcase-el/liteflow-testcase-el-springboot4/src/test/java/com/yomahub/liteflow/test/config \
  liteflow-testcase-el/liteflow-testcase-el-solon/src/test/java/com/yomahub/liteflow/test/config
git commit -m "refactor(config): group rule-db backend settings"
```

### Task 8: Adapt SQL to Provider, Polling, and Unified Publisher

**Files:**
- Create: `liteflow-rule-db/liteflow-rule-db-sql/src/main/java/com/yomahub/liteflow/repository/sql/SqlRuleDbProvider.java`
- Create: `liteflow-rule-db/liteflow-rule-db-sql/src/main/java/com/yomahub/liteflow/repository/sql/SqlPollingChangeSource.java`
- Create: `liteflow-rule-db/liteflow-rule-db-sql/src/main/java/com/yomahub/liteflow/repository/sql/SqlPublisherConfig.java`
- Create: `liteflow-rule-db/liteflow-rule-db-sql/src/main/java/com/yomahub/liteflow/repository/sql/SqlRulePublisherImpl.java`
- Create: `liteflow-rule-db/liteflow-rule-db-sql/src/main/java/com/yomahub/liteflow/repository/sql/SqlRulePublisherProvider.java`
- Modify: `liteflow-rule-db/liteflow-rule-db-sql/src/main/java/com/yomahub/liteflow/repository/sql/SqlRuleRepository.java`
- Modify: `liteflow-rule-db/liteflow-rule-db-sql/src/main/java/com/yomahub/liteflow/repository/sql/SqlConnectionManager.java`
- Modify: `liteflow-rule-db/liteflow-rule-db-sql/src/main/java/com/yomahub/liteflow/repository/sql/SqlDialect.java`
- Delete: `liteflow-rule-db/liteflow-rule-db-sql/src/main/java/com/yomahub/liteflow/repository/sql/SqlRulePublisher.java`
- Delete: `liteflow-rule-db/liteflow-rule-db-sql/src/main/resources/META-INF/services/com.yomahub.liteflow.repository.RuleRepository`
- Create: `liteflow-rule-db/liteflow-rule-db-sql/src/main/resources/META-INF/services/com.yomahub.liteflow.repository.RuleDbProvider`
- Create: `liteflow-rule-db/liteflow-rule-db-sql/src/main/resources/META-INF/services/com.yomahub.liteflow.publisher.RulePublisherProvider`
- Test: `liteflow-testcase-el/liteflow-testcase-el-rule-db-sql-springboot/src/test/java/com/yomahub/liteflow/test/ruledb/sql/SqlRepositoryProtocolTest.java`
- Test: `liteflow-testcase-el/liteflow-testcase-el-rule-db-sql-springboot/src/test/java/com/yomahub/liteflow/test/ruledb/sql/RuleDbSqlTest.java`
- Create: `liteflow-testcase-el/liteflow-testcase-el-rule-db-sql-springboot/src/test/java/com/yomahub/liteflow/test/ruledb/sql/SqlPublisherContractTest.java`

- [ ] **Step 1: Add failing SQL provider and Publisher contract tests**

Test create-only `expectedVersion = 0`, exact-version update, conflict rollback, delete conflict, `fetchChainMeta`, `fetchScriptMeta`, and `SqlPollingChangeSource.pollOnce()`.

```java
assertThrows(VersionConflictException.class,
        () -> publisher.publishChain(chain("c1", "THEN(b)", 7L)));
assertEquals("THEN(a)", repository.fetchChain("c1").getEl());
assertEquals(1, repository.fetchLatestSeqForTest());
```

- [ ] **Step 2: Run and verify failure**

Run: `mvn test -pl liteflow-testcase-el/liteflow-testcase-el-rule-db-sql-springboot -am`

Expected: new tests fail because SQL still registers `RuleRepository` and exposes the old publisher.

- [ ] **Step 3: Implement SQL adapters**

Use one `SqlConnectionManager` per Provider/Publisher instance. `SqlRuleDbProvider` returns `SqlRuleRepository` and `SqlPollingChangeSource`. The polling source schedules fixed-delay polling after `activate`, advances only after applying a complete ordered batch, and turns `SeqGapException` into an immediate reconciliation callback.

`SqlRulePublisherProvider` accepts only `SqlPublisherConfig`. Publisher uses conditional `UPDATE ... WHERE version = ?` for expected versions, inserts on expected version zero, and writes change log in the same transaction. Return the generated business version and sequence.

- [ ] **Step 4: Run SQL integration tests**

Run: `mvn test -pl liteflow-testcase-el/liteflow-testcase-el-rule-db-sql-springboot -am`

Expected: SQL protocol, runtime, publisher, manual version update, and existing script regression tests pass.

- [ ] **Step 5: Commit**

```bash
git add liteflow-rule-db/liteflow-rule-db-sql \
  liteflow-testcase-el/liteflow-testcase-el-rule-db-sql-springboot
git commit -m "refactor(rule-db-sql): use unified provider and publisher"
```

### Task 9: Remove Redis Pub/Sub and Match SQL Polling

**Files:**
- Create: `liteflow-rule-db/liteflow-rule-db-redis/src/main/java/com/yomahub/liteflow/repository/redis/RedisRuleDbProvider.java`
- Create: `liteflow-rule-db/liteflow-rule-db-redis/src/main/java/com/yomahub/liteflow/repository/redis/RedisPollingChangeSource.java`
- Create: `liteflow-rule-db/liteflow-rule-db-redis/src/main/java/com/yomahub/liteflow/repository/redis/RedisPublisherConfig.java`
- Create: `liteflow-rule-db/liteflow-rule-db-redis/src/main/java/com/yomahub/liteflow/repository/redis/RedisRulePublisherImpl.java`
- Create: `liteflow-rule-db/liteflow-rule-db-redis/src/main/java/com/yomahub/liteflow/repository/redis/RedisRulePublisherProvider.java`
- Modify: `liteflow-rule-db/liteflow-rule-db-redis/src/main/java/com/yomahub/liteflow/repository/redis/RedisRuleRepository.java`
- Modify: `liteflow-rule-db/liteflow-rule-db-redis/src/main/java/com/yomahub/liteflow/repository/redis/RedisKeys.java`
- Modify: `liteflow-rule-db/liteflow-rule-db-redis/src/main/java/com/yomahub/liteflow/repository/redis/RedisConnectionManager.java`
- Modify: `liteflow-rule-db/liteflow-rule-db-redis/src/main/resources/lua/publish-chain.lua`
- Modify: `liteflow-rule-db/liteflow-rule-db-redis/src/main/resources/lua/publish-script.lua`
- Modify: `liteflow-rule-db/liteflow-rule-db-redis/src/main/resources/lua/remove.lua`
- Delete: `liteflow-rule-db/liteflow-rule-db-redis/src/main/java/com/yomahub/liteflow/repository/redis/RedisRulePublisher.java`
- Delete: `liteflow-rule-db/liteflow-rule-db-redis/src/main/resources/META-INF/services/com.yomahub.liteflow.repository.RuleRepository`
- Create: `liteflow-rule-db/liteflow-rule-db-redis/src/main/resources/META-INF/services/com.yomahub.liteflow.repository.RuleDbProvider`
- Create: `liteflow-rule-db/liteflow-rule-db-redis/src/main/resources/META-INF/services/com.yomahub.liteflow.publisher.RulePublisherProvider`
- Test: `liteflow-testcase-el/liteflow-testcase-el-rule-db-redis-springboot/src/test/java/com/yomahub/liteflow/test/ruledb/redis/RuleDbRedisTest.java`
- Create: `liteflow-testcase-el/liteflow-testcase-el-rule-db-redis-springboot/src/test/java/com/yomahub/liteflow/test/ruledb/redis/RedisPublisherContractTest.java`

- [ ] **Step 1: Add failing no-pubsub and data-model tests**

Assert Redis Provider reports polling mode, default interval is 3 seconds, no listener is registered on a topic, ID sets contain IDs only, manifest reads metadata fields without `HGETALL`, manual `HSET version` converges through reconcile, and publisher conflict leaves Hash, Set, seq, and ZSet unchanged.

- [ ] **Step 2: Run and verify failure**

Run: `mvn test -pl liteflow-testcase-el/liteflow-testcase-el-rule-db-redis-springboot -am`

Expected: tests fail because Redis registers pub/sub, defaults to 30 seconds, and duplicates version in Hash indexes.

- [ ] **Step 3: Implement the Redis polling model**

Change indexes from Hashes to `chain-ids` and `script-ids` Sets. Build manifest with pipelined `HMGET` calls for metadata fields. Remove `notifyChannel`, `RTopic`, `subscribe`, the fifth Lua key, and every `PUBLISH` call.

Lua publish order must be:

```lua
-- validate expected version before writes
redis.call('HSET', contentKey, ...)
redis.call('SADD', idSetKey, targetId)
local seq = redis.call('INCR', seqKey)
redis.call('ZADD', changelogKey, seq, changeJson)
return {version, seq}
```

Delete performs `DEL`, `SREM`, `INCR`, and `ZADD` atomically. Redis polling interval defaults to the same 3 seconds as SQL.

- [ ] **Step 4: Run Redis and SQL tests together**

Run: `mvn test -pl liteflow-testcase-el/liteflow-testcase-el-rule-db-sql-springboot,liteflow-testcase-el/liteflow-testcase-el-rule-db-redis-springboot -am`

Expected: both suites pass with identical polling and reconciliation semantics.

- [ ] **Step 5: Commit**

```bash
git add liteflow-rule-db/liteflow-rule-db-redis \
  liteflow-testcase-el/liteflow-testcase-el-rule-db-redis-springboot
git commit -m "refactor(rule-db-redis): replace pubsub with polling"
```

### Task 10: Remove the Transitional Repository API and Run Baseline Regression

**Files:**
- Modify: `liteflow-core/src/main/java/com/yomahub/liteflow/repository/RuleRepository.java`
- Modify: `liteflow-core/src/main/java/com/yomahub/liteflow/repository/RuleChangeListener.java`
- Modify: `liteflow-core/src/main/java/com/yomahub/liteflow/repository/vo/ChangeRecord.java`
- Delete: `liteflow-core/src/main/java/com/yomahub/liteflow/repository/RuleRepositoryHolder.java`
- Modify: `liteflow-testcase-el/liteflow-testcase-el-rule-db-core/src/test/java/com/yomahub/liteflow/test/ruledb/BaseRuleDbTest.java`
- Modify: `liteflow-testcase-el/liteflow-testcase-el-rule-db-core/src/test/java/com/yomahub/liteflow/test/ruledb/InMemoryRuleRepository.java`

- [ ] **Step 1: Search for legacy API use**

Run:

```bash
rg -n 'RuleRepositoryHolder|\.subscribe\(|defaultSeqPollSeconds|fetchLatestSeq\(|fetchChangesSince\(' \
  liteflow-core liteflow-rule-db liteflow-testcase-el/liteflow-testcase-el-rule-db-*
```

Expected: only transitional declarations and tests remain; production SQL/Redis paths use Provider and ChangeSource.

- [ ] **Step 2: Delete transitional methods and holder**

Final `RuleRepository` contains only manifest, per-target metadata, and per-target content reads. Delete `RuleRepositoryHolder`; update cleanup to reset `RuleDbProviderHolder`.

- [ ] **Step 3: Compile production modules**

Run: `mvn clean package -DskipTests -pl liteflow-core,liteflow-rule-db -am`

Expected: BUILD SUCCESS.

- [ ] **Step 4: Run the complete baseline test set**

Run:

```bash
mvn test -pl liteflow-testcase-el/liteflow-testcase-el-rule-db-core,\
liteflow-testcase-el/liteflow-testcase-el-rule-db-sql-springboot,\
liteflow-testcase-el/liteflow-testcase-el-rule-db-redis-springboot -am
```

Expected: all three Rule-DB suites pass with zero failures and errors.

- [ ] **Step 5: Commit**

```bash
git add liteflow-core liteflow-rule-db liteflow-testcase-el/liteflow-testcase-el-rule-db-*
git commit -m "refactor(rule-db): complete unified sql and redis baseline"
```
