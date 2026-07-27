# Rule-DB Configuration, Observability, Documentation, and Regression Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Finish the four-backend Rule-DB feature with grouped configuration, runtime health visibility, complete user documentation, and full regression verification.

**Architecture:** Keep execution configuration in core-owned nested POJOs so Spring Boot, Spring Boot 4, and Solon bind the same model without backend dependencies. Expose a read-only runtime snapshot through the metrics view, document Publisher and versioned manual updates, then verify new and traditional plugins together.

**Tech Stack:** Java, Maven, Spring Boot configuration properties and actuator, Spring Boot 4, Solon, Micrometer-facing metadata view, JSON configuration metadata, Markdown.

---

**Prerequisites:** Complete the core/SQL/Redis, etcd, and ZK plans. Preserve and merge the pre-existing uncommitted changes in `docs/liteflow-rule-db-guide.md` rather than replacing the file wholesale.

## File Map

- IDE metadata: both `additional-spring-configuration-metadata.json` files
- Runtime snapshot: core repository package and `liteflow-metrics/LiteflowMetaView.java`
- Endpoint tests: existing Spring Boot and Spring Boot 4 metrics tests
- Documentation: `docs/liteflow-rule-db-guide.md` and `AGENTS.md`

### Task 1: Update Spring Configuration Metadata

**Files:**
- Modify: `liteflow-spring-boot-starter/src/main/resources/META-INF/additional-spring-configuration-metadata.json`
- Modify: `liteflow-spring-boot4-starter/src/main/resources/META-INF/additional-spring-configuration-metadata.json`
- Test: metadata validation command

- [ ] **Step 1: Replace flat entries with the grouped property set**

Include every public field from the six core config classes. Required names include:

```text
liteflow.rule-db.cache.capacity
liteflow.rule-db.cache.preload-chain-ids
liteflow.rule-db.sync.poll-seconds
liteflow.rule-db.sync.reconcile-seconds
liteflow.rule-db.sync.fetch-retry-times
liteflow.rule-db.sql.*
liteflow.rule-db.redis.*
liteflow.rule-db.etcd.*
liteflow.rule-db.zk.*
```

Descriptions must state that SQL/Redis poll and etcd/ZK watch, and that all four reconcile.

- [ ] **Step 2: Parse and compare the two metadata files**

Run:

```bash
jq empty liteflow-spring-boot-starter/src/main/resources/META-INF/additional-spring-configuration-metadata.json
jq empty liteflow-spring-boot4-starter/src/main/resources/META-INF/additional-spring-configuration-metadata.json
diff <(jq -S '.properties[] | select(.name | startswith("liteflow.rule-db"))' liteflow-spring-boot-starter/src/main/resources/META-INF/additional-spring-configuration-metadata.json) \
     <(jq -S '.properties[] | select(.name | startswith("liteflow.rule-db"))' liteflow-spring-boot4-starter/src/main/resources/META-INF/additional-spring-configuration-metadata.json)
```

Expected: both JSON files parse and the Rule-DB entries are identical.

- [ ] **Step 3: Build both starters**

Run: `mvn package -DskipTests -pl liteflow-spring-boot-starter,liteflow-spring-boot4-starter -am`

Expected: BUILD SUCCESS.

- [ ] **Step 4: Commit**

```bash
git add liteflow-spring-boot-starter/src/main/resources/META-INF/additional-spring-configuration-metadata.json \
  liteflow-spring-boot4-starter/src/main/resources/META-INF/additional-spring-configuration-metadata.json
git commit -m "docs(config): publish grouped rule-db metadata"
```

### Task 2: Expose Rule-DB Runtime Health

**Files:**
- Create: `liteflow-core/src/main/java/com/yomahub/liteflow/repository/vo/RuleDbRuntimeSnapshot.java`
- Modify: `liteflow-core/src/main/java/com/yomahub/liteflow/repository/RuleDbRuntime.java`
- Modify: `liteflow-core/src/main/java/com/yomahub/liteflow/repository/RuleDbSyncManager.java`
- Modify: `liteflow-metrics/src/main/java/com/yomahub/liteflow/metrics/LiteflowMetaView.java`
- Modify: `liteflow-spring-boot-starter/src/main/java/com/yomahub/liteflow/springboot/metrics/LiteflowEndpoint.java`
- Modify: `liteflow-spring-boot4-starter/src/main/java/com/yomahub/liteflow/springboot4/metrics/LiteflowEndpoint.java`
- Modify: `liteflow-testcase-el/liteflow-testcase-el-springboot/src/test/java/com/yomahub/liteflow/test/metrics/MetricsEndpointSpringbootTest.java`
- Modify: `liteflow-testcase-el/liteflow-testcase-el-springboot4/src/test/java/com/yomahub/liteflow/test/metrics/MetricsEndpointSpringboot4Test.java`

- [ ] **Step 1: Add failing endpoint tests**

```java
mockMvc.perform(get("/actuator/liteflow/ruledb"))
    .andExpect(status().isOk())
    .andExpect(jsonPath("$.provider").value("sql"))
    .andExpect(jsonPath("$.changeSource.status").value("UP"))
    .andExpect(jsonPath("$.targets.ready").isNumber())
    .andExpect(jsonPath("$.lastAppliedSeq").isNumber());
```

- [ ] **Step 2: Run and verify failure**

Run:

```bash
mvn test -pl liteflow-testcase-el/liteflow-testcase-el-springboot,\
liteflow-testcase-el/liteflow-testcase-el-springboot4 \
  -Dtest='*MetricsEndpoint*Test'
```

Expected: endpoint selector returns unknown selector because `ruledb` is not exposed.

- [ ] **Step 3: Add a read-only snapshot**

Snapshot fields include provider type, ChangeSource health, last applied seq, last successful reconcile time, last reconcile error summary, counts for SHADOW/READY/STALE/LOADING/FAILED/DELETED, and a bounded list of failed targets containing IDs and versions but no content or credentials.

`LiteflowMetaView.ruleDb()` maps the snapshot to an ordered map. Both endpoints route selector `ruledb` to this method. When Rule-DB is inactive, return `{ "active": false }`.

- [ ] **Step 4: Run endpoint and core state tests**

Run:

```bash
mvn test -pl liteflow-testcase-el/liteflow-testcase-el-rule-db-core,\
liteflow-testcase-el/liteflow-testcase-el-springboot,\
liteflow-testcase-el/liteflow-testcase-el-springboot4 \
  -Dtest='RuleDb*Test,*MetricsEndpoint*Test' -am
```

Expected: all selected tests pass and endpoint output contains no rule body.

- [ ] **Step 5: Commit**

```bash
git add liteflow-core/src/main/java/com/yomahub/liteflow/repository \
  liteflow-metrics liteflow-spring-boot-starter liteflow-spring-boot4-starter \
  liteflow-testcase-el
git commit -m "feat(metrics): expose rule-db synchronization health"
```

### Task 3: Rewrite the Rule-DB User Guide

**Files:**
- Modify: `docs/liteflow-rule-db-guide.md`
- Modify: `AGENTS.md`

- [ ] **Step 1: Update architecture and dependency sections**

Document all five artifacts: Publisher plus four DB modules. Clearly distinguish traditional `liteflow-rule-etcd`/`liteflow-rule-zk` from new `liteflow-rule-db-etcd`/`liteflow-rule-db-zk`.

- [ ] **Step 2: Add unified Publisher examples**

Show a management application depending on Publisher plus one DB module, typed backend configuration, `RulePublisherFactory.create`, try-with-resources, publish chain/script, remove, and `expectedVersion` conflict handling.

- [ ] **Step 3: Replace synchronization and manual-update guidance**

State exactly:

```text
SQL/Redis = change-log polling + manifest reconciliation
etcd/ZK = metadata watch + manifest reconciliation
Manual content edits must also increment manifest-visible business version.
Change-log or notification writes are optional for manual edits; omission delays convergence to reconciliation.
```

For Redis, document ID Sets and same-Hash version. For etcd/ZK, document separate metadata/content keys and updating metadata last. Preserve the existing correction explaining that content-only edits with unchanged version and md5 are not detectable.

- [ ] **Step 4: Validate documentation references**

Run:

```bash
rg -n 'Redis.*pub/sub|RTopic|PUBLISH|Redis 默认 `30`|HSET index' docs/liteflow-rule-db-guide.md
rg -n 'liteflow-rule-db-(publisher|sql|redis|etcd|zk)' docs/liteflow-rule-db-guide.md
```

Expected: first search has no obsolete mechanism claims; second finds all five artifacts in dependency and reference sections.

- [ ] **Step 5: Commit**

```bash
git add docs/liteflow-rule-db-guide.md AGENTS.md
git commit -m "docs: document unified four-backend rule-db"
```

`AGENTS.md` currently contains the Rule-DB module list and must be updated from two implementations to four plus Publisher. The root README files do not currently list Rule-DB modules and must remain untouched.

### Task 4: Run Structural and Full Regression Verification

**Files:**
- No production edits expected. A failure returns execution to the task that owns the failing behavior; rerun this verification task only after that focused fix is committed.

- [ ] **Step 1: Verify the removed Redis mechanism and SPI topology**

Run:

```bash
! rg -n 'RTopic|notifyChannel|redis.call\(.PUBLISH.|void subscribe\(' liteflow-rule-db/liteflow-rule-db-redis liteflow-core/src/main/java/com/yomahub/liteflow/repository
find liteflow-rule-db -path '*/META-INF/services/*' -type f -maxdepth 8 -print -exec sed -n '1,20p' {} \;
```

Expected: no Redis notification matches; each DB module registers one RuleDbProvider and one RulePublisherProvider.

- [ ] **Step 2: Build production modules**

Run: `mvn clean package -DskipTests -pl liteflow-core,liteflow-rule-db,liteflow-metrics,liteflow-spring-boot-starter,liteflow-spring-boot4-starter,liteflow-solon-plugin -am`

Expected: BUILD SUCCESS.

- [ ] **Step 3: Run all Rule-DB suites**

Run:

```bash
mvn test -pl liteflow-testcase-el/liteflow-testcase-el-rule-db-core,\
liteflow-testcase-el/liteflow-testcase-el-rule-db-sql-springboot,\
liteflow-testcase-el/liteflow-testcase-el-rule-db-redis-springboot,\
liteflow-testcase-el/liteflow-testcase-el-rule-db-etcd-springboot,\
liteflow-testcase-el/liteflow-testcase-el-rule-db-zk-springboot -am
```

Expected: zero failures and errors. Record a Docker-unavailable etcd failure explicitly; do not describe the etcd suite as passing unless the container test actually ran.

- [ ] **Step 4: Run traditional plugin regressions**

Run:

```bash
mvn test -pl liteflow-testcase-el/liteflow-testcase-el-etcd-springboot,\
liteflow-testcase-el/liteflow-testcase-el-zk-springboot,\
liteflow-testcase-el/liteflow-testcase-el-sql-springboot,\
liteflow-testcase-el/liteflow-testcase-el-redis-springboot -am
```

Expected: traditional full-load plugins remain green and use their original artifact IDs.

- [ ] **Step 5: Run the repository build appropriate for JDK 17**

Run: `mvn clean package`

Expected: BUILD SUCCESS with all active JDK 17 profiles and tests passing.

- [ ] **Step 6: Confirm verification introduced no uncommitted files**

```bash
git status --short
```

Expected: only changes that were already present before plan execution are listed; verification itself leaves no new files or edits.
