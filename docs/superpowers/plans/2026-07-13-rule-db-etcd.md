# Rule-DB etcd Backend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an etcd-backed Rule-DB provider and unified Publisher implementation that load metadata at startup, content on demand, and propagate changes through native watch.

**Architecture:** Separate metadata and content prefixes so manifest range queries never read rule bodies. Open and buffer watches before fetching the manifest, activate from the snapshot revision, and recover compacted revisions through immediate reconciliation.

**Tech Stack:** Java, Maven, jetcd 0.7.3, Jackson, ServiceLoader, JUnit 5, Testcontainers GenericContainer with etcd.

---

**Prerequisite:** Complete `2026-07-13-rule-db-core-publisher-sql-redis.md` and keep traditional `liteflow-rule-plugin/liteflow-rule-etcd` unchanged.

## File Map

- Production module: new `liteflow-rule-db/liteflow-rule-db-etcd/`
- Repository package: `com.yomahub.liteflow.repository.etcd`
- Publisher config: `EtcdPublisherConfig`
- Service resources: `RuleDbProvider` and `RulePublisherProvider`
- Integration module: new `liteflow-testcase-el-rule-db-etcd-springboot`

### Task 1: Scaffold the Module, Configuration, Keys, and Codec

**Files:**
- Modify: `liteflow-rule-db/pom.xml`
- Create: `liteflow-rule-db/liteflow-rule-db-etcd/pom.xml`
- Create: `liteflow-rule-db/liteflow-rule-db-etcd/src/main/java/com/yomahub/liteflow/repository/etcd/EtcdPublisherConfig.java`
- Create: `liteflow-rule-db/liteflow-rule-db-etcd/src/main/java/com/yomahub/liteflow/repository/etcd/EtcdKeys.java`
- Create: `liteflow-rule-db/liteflow-rule-db-etcd/src/main/java/com/yomahub/liteflow/repository/etcd/EtcdRecordCodec.java`
- Create: `liteflow-rule-db/liteflow-rule-db-etcd/src/main/java/com/yomahub/liteflow/repository/etcd/EtcdConnectionManager.java`
- Create: `liteflow-rule-db/liteflow-rule-db-etcd/src/test/java/com/yomahub/liteflow/repository/etcd/EtcdKeysTest.java`
- Create: `liteflow-rule-db/liteflow-rule-db-etcd/src/test/java/com/yomahub/liteflow/repository/etcd/EtcdRecordCodecTest.java`

- [ ] **Step 1: Add failing key-layout tests**

```java
@Test
void buildsIsolatedMetaAndContentKeys() {
    EtcdKeys keys = new EtcdKeys("/liteflow/", "order-app");
    assertEquals("/liteflow/order-app/chains/meta/c1", keys.chainMeta("c1"));
    assertEquals("/liteflow/order-app/chains/content/c1", keys.chainContent("c1"));
    assertEquals("/liteflow/order-app/scripts/meta/s1", keys.scriptMeta("s1"));
}
```

- [ ] **Step 2: Run and verify failure**

Run: `mvn test -pl liteflow-rule-db/liteflow-rule-db-etcd -am -Dtest=EtcdKeysTest`

Expected: Maven fails because the module does not exist.

- [ ] **Step 3: Add module dependencies and focused value objects**

Depend on `liteflow-core`, `liteflow-rule-db-publisher`, `jetcd-core`, and Jackson. Execution uses core's `RuleDbEtcdConfig`; Publisher uses the explicit backend `EtcdPublisherConfig`. Normalize root path once and reject blank endpoints/application names. `EtcdRecordCodec` serializes metadata separately from content and rejects unknown target types or missing version.

```java
final class EtcdKeys {
    String chainMetaPrefix() { return base + "/chains/meta/"; }
    String chainContentPrefix() { return base + "/chains/content/"; }
    String scriptMetaPrefix() { return base + "/scripts/meta/"; }
    String scriptContentPrefix() { return base + "/scripts/content/"; }
}
```

- [ ] **Step 4: Run unit tests**

Run: `mvn test -pl liteflow-rule-db/liteflow-rule-db-etcd -am`

Expected: module compiles and key/codec tests pass.

- [ ] **Step 5: Commit**

```bash
git add liteflow-rule-db/pom.xml liteflow-rule-db/liteflow-rule-db-etcd
git commit -m "feat(rule-db-etcd): scaffold backend and storage protocol"
```

### Task 2: Implement Metadata-Only Repository Reads

**Files:**
- Create: `liteflow-rule-db/liteflow-rule-db-etcd/src/main/java/com/yomahub/liteflow/repository/etcd/EtcdRuleRepository.java`
- Create: `liteflow-rule-db/liteflow-rule-db-etcd/src/main/java/com/yomahub/liteflow/repository/etcd/EtcdKvFacade.java`
- Create: `liteflow-rule-db/liteflow-rule-db-etcd/src/test/java/com/yomahub/liteflow/repository/etcd/FakeEtcdKvFacade.java`
- Create: `liteflow-rule-db/liteflow-rule-db-etcd/src/test/java/com/yomahub/liteflow/repository/etcd/EtcdRuleRepositoryTest.java`

- [ ] **Step 1: Add failing repository protocol tests**

```java
@Test
void manifestRangesOnlyMetaPrefixes() {
    RuleManifest manifest = repository.fetchManifest();
    assertEquals(Arrays.asList(keys.chainMetaPrefix(), keys.scriptMetaPrefix()), fake.rangePrefixes());
    assertEquals(0, fake.contentReadCount());
    assertEquals(42L, manifest.getLatestSeq());
}

@Test
void fetchChainRejectsMetaContentVersionMismatch() {
    fake.putChainMeta("c1", 2, "md5-v2");
    fake.putChainContent("c1", 1, "THEN(a)");
    assertThrows(RuleStorageException.class, () -> repository.fetchChain("c1"));
}
```

- [ ] **Step 2: Run and verify failure**

Run: `mvn test -pl liteflow-rule-db/liteflow-rule-db-etcd -am -Dtest=EtcdRuleRepositoryTest`

Expected: tests fail because `EtcdRuleRepository` does not exist.

- [ ] **Step 3: Implement repository reads**

Use one range response for each metadata prefix and take the greatest response header revision as `RuleManifest.latestSeq`. `fetchChainMeta` and `fetchScriptMeta` perform exact metadata gets. Content reads perform metadata-content-metadata reads and accept the result only when both metadata reads have the same business version and md5.

Do not reuse the traditional parser's XML assembly or `EtcdParserHelper`; the new module returns Rule-DB VO objects only.

- [ ] **Step 4: Run repository tests**

Run: `mvn test -pl liteflow-rule-db/liteflow-rule-db-etcd -am -Dtest=EtcdRuleRepositoryTest`

Expected: metadata-only and version-consistency tests pass.

- [ ] **Step 5: Commit**

```bash
git add liteflow-rule-db/liteflow-rule-db-etcd
git commit -m "feat(rule-db-etcd): add lazy metadata repository"
```

### Task 3: Implement Buffered Watch and Compaction Recovery

**Files:**
- Create: `liteflow-rule-db/liteflow-rule-db-etcd/src/main/java/com/yomahub/liteflow/repository/etcd/EtcdWatchChangeSource.java`
- Create: `liteflow-rule-db/liteflow-rule-db-etcd/src/main/java/com/yomahub/liteflow/repository/etcd/EtcdRuleDbProvider.java`
- Create: `liteflow-rule-db/liteflow-rule-db-etcd/src/main/resources/META-INF/services/com.yomahub.liteflow.repository.RuleDbProvider`
- Create: `liteflow-rule-db/liteflow-rule-db-etcd/src/test/java/com/yomahub/liteflow/repository/etcd/EtcdWatchChangeSourceTest.java`

- [ ] **Step 1: Add failing two-phase watch tests**

```java
@Test
void buffersBeforeActivateAndDropsSnapshotEvents() {
    source.open(changes::addAll);
    fakeWatch.emitPut(meta("c1", 1), 10);
    fakeWatch.emitPut(meta("c1", 2), 12);
    source.activate(10);
    assertEquals(Collections.singletonList(change(12, "c1", 2)), changes);
}

@Test
void compactedRevisionRequestsReconcileAndRestarts() {
    source.open(changes::addAll);
    source.activate(20);
    fakeWatch.failCompacted(20);
    assertEquals(1, reconcileRequests.get());
    assertEquals(ChangeSourceHealth.Status.DEGRADED, source.health().getStatus());
}
```

- [ ] **Step 2: Run and verify failure**

Run: `mvn test -pl liteflow-rule-db/liteflow-rule-db-etcd -am -Dtest=EtcdWatchChangeSourceTest`

Expected: tests fail because watch source and provider are absent.

- [ ] **Step 3: Implement watch behavior**

Open one prefix watch for chain metadata and one for script metadata. Convert PUT to UPSERT by decoding metadata; convert DELETE using the key ID and event revision. Buffer in a sequence-ordered map until activation, discard events at or below the baseline, and serialize listener delivery.

On transport failure, reconnect with bounded exponential backoff. On compacted revision, close both watchers, request immediate reconcile through the core callback, and reopen from the new baseline supplied after reconciliation. Close is idempotent and makes late callbacks no-op.

- [ ] **Step 4: Run watch tests**

Run: `mvn test -pl liteflow-rule-db/liteflow-rule-db-etcd -am -Dtest=EtcdWatchChangeSourceTest`

Expected: buffering, ordering, delete, close, reconnect, and compaction tests pass.

- [ ] **Step 5: Commit**

```bash
git add liteflow-rule-db/liteflow-rule-db-etcd
git commit -m "feat(rule-db-etcd): add buffered watch change source"
```

### Task 4: Implement Transactional Unified Publisher

**Files:**
- Create: `liteflow-rule-db/liteflow-rule-db-etcd/src/main/java/com/yomahub/liteflow/repository/etcd/EtcdRulePublisher.java`
- Create: `liteflow-rule-db/liteflow-rule-db-etcd/src/main/java/com/yomahub/liteflow/repository/etcd/EtcdRulePublisherProvider.java`
- Create: `liteflow-rule-db/liteflow-rule-db-etcd/src/main/resources/META-INF/services/com.yomahub.liteflow.publisher.RulePublisherProvider`
- Create: `liteflow-rule-db/liteflow-rule-db-etcd/src/test/java/com/yomahub/liteflow/repository/etcd/EtcdRulePublisherTest.java`

- [ ] **Step 1: Add failing Publisher contract tests**

Cover unconditional create/update, expected zero create-only, exact-version update, conflict with no writes, atomic metadata/content delete, md5 calculation, and returned etcd revision.

```java
PublishResult result = publisher.publishChain(chain("c1", "THEN(a)", 0L));
assertEquals(1L, result.getVersion());
assertTrue(result.getSequence() > 0);
assertThrows(VersionConflictException.class,
        () -> publisher.publishChain(chain("c1", "THEN(b)", 7L)));
```

- [ ] **Step 2: Run and verify failure**

Run: `mvn test -pl liteflow-rule-db/liteflow-rule-db-etcd -am -Dtest=EtcdRulePublisherTest`

Expected: tests fail because no etcd Publisher provider is registered.

- [ ] **Step 3: Implement compare-and-transaction writes**

Read current metadata once, validate expected business version, then create a transaction comparing the metadata key's mod revision. Put content first and metadata second in the same transaction; the response header revision is the returned sequence. Retry an unconditional publish when CAS loses a race. For delete, compare metadata mod revision and delete both keys atomically.

- [ ] **Step 4: Run Publisher tests**

Run: `mvn test -pl liteflow-rule-db/liteflow-rule-db-etcd -am -Dtest=EtcdRulePublisherTest,EtcdRuleRepositoryTest,EtcdWatchChangeSourceTest`

Expected: all etcd unit contract tests pass.

- [ ] **Step 5: Commit**

```bash
git add liteflow-rule-db/liteflow-rule-db-etcd
git commit -m "feat(rule-db-etcd): add unified transactional publisher"
```

### Task 5: Add Real etcd Runtime Integration Tests

**Files:**
- Modify: `liteflow-testcase-el/pom.xml`
- Create: `liteflow-testcase-el/liteflow-testcase-el-rule-db-etcd-springboot/pom.xml`
- Create: `liteflow-testcase-el/liteflow-testcase-el-rule-db-etcd-springboot/src/test/java/com/yomahub/liteflow/test/ruledb/etcd/RuleDbEtcdApplication.java`
- Create: `liteflow-testcase-el/liteflow-testcase-el-rule-db-etcd-springboot/src/test/java/com/yomahub/liteflow/test/ruledb/etcd/RuleDbEtcdTest.java`
- Create: `liteflow-testcase-el/liteflow-testcase-el-rule-db-etcd-springboot/src/test/java/com/yomahub/liteflow/test/ruledb/etcd/cmp/ACmp.java`
- Create: `liteflow-testcase-el/liteflow-testcase-el-rule-db-etcd-springboot/src/test/java/com/yomahub/liteflow/test/ruledb/etcd/cmp/BCmp.java`
- Create: `liteflow-testcase-el/liteflow-testcase-el-rule-db-etcd-springboot/src/test/resources/application.properties`

- [ ] **Step 1: Add a Testcontainers-backed failing integration test**

Add test-scoped `org.testcontainers:testcontainers:1.20.6` and `org.testcontainers:junit-jupiter:1.20.6`. Start `quay.io/coreos/etcd:v3.5.12` with `GenericContainer`, publish chain and script through `RulePublisherFactory`, then assert startup body-read count is zero, first execution succeeds, watch update converges, and a direct metadata version update without a Publisher also converges.

- [ ] **Step 2: Run and verify the testcase initially fails**

Run: `mvn test -pl liteflow-testcase-el/liteflow-testcase-el-rule-db-etcd-springboot -am`

Expected: test fails on the first incomplete integration behavior; if Docker is unavailable, Maven reports the explicit Testcontainers environment error rather than silently skipping.

- [ ] **Step 3: Complete configuration wiring and lifecycle cleanup**

Use dynamic properties for the mapped endpoint and application name. Close Publisher, FlowExecutor runtime, jetcd client, and container in reverse order. Use bounded condition polling rather than fixed sleeps for watch convergence.

- [ ] **Step 4: Run new and legacy etcd tests**

Run:

```bash
mvn test -pl liteflow-testcase-el/liteflow-testcase-el-rule-db-etcd-springboot,\
liteflow-testcase-el/liteflow-testcase-el-etcd-springboot -am
```

Expected: new Rule-DB tests pass and traditional `liteflow-rule-etcd` tests remain unchanged.

- [ ] **Step 5: Commit**

```bash
git add liteflow-testcase-el/pom.xml \
  liteflow-testcase-el/liteflow-testcase-el-rule-db-etcd-springboot
git commit -m "test(rule-db-etcd): cover lazy load watch and manual updates"
```
