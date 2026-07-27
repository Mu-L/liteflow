# Rule-DB Parent Module Relocation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the SQL and Redis Rule-DB modules out of `liteflow-rule-plugin` and aggregate them under a new root-level `liteflow-rule-db` parent module without changing their published coordinates or runtime behavior.

**Architecture:** Add `liteflow-rule-db` as a sibling reactor module of `liteflow-rule-plugin`. The new parent POM owns the two existing Rule-DB implementations, while the root profiles include both parent modules independently. Repository documentation is updated so every source path and ownership statement matches the new layout.

**Tech Stack:** Maven multi-module reactor, Java 8-compatible modules, Markdown documentation, Git

---

### Task 1: Move Rule-DB modules and establish the new Maven parent

**Files:**
- Create: `liteflow-rule-db/pom.xml`
- Move: `liteflow-rule-plugin/liteflow-rule-db-sql/` to `liteflow-rule-db/liteflow-rule-db-sql/`
- Move: `liteflow-rule-plugin/liteflow-rule-db-redis/` to `liteflow-rule-db/liteflow-rule-db-redis/`
- Modify: `liteflow-rule-db/liteflow-rule-db-sql/pom.xml`
- Modify: `liteflow-rule-db/liteflow-rule-db-redis/pom.xml`
- Modify: `liteflow-rule-plugin/pom.xml`
- Modify: `pom.xml`

- [ ] **Step 1: Capture the pre-migration Maven model failure for the future path**

Run:

```bash
mvn -N help:evaluate -f liteflow-rule-db/pom.xml -Dexpression=project.modules -q -DforceStdout
```

Expected: FAIL because `liteflow-rule-db/pom.xml` does not exist yet.

- [ ] **Step 2: Move both complete module directories**

Run:

```bash
mkdir liteflow-rule-db
git mv liteflow-rule-plugin/liteflow-rule-db-sql liteflow-rule-db/
git mv liteflow-rule-plugin/liteflow-rule-db-redis liteflow-rule-db/
```

Expected: both source trees, resources, service registrations and child POMs are staged as renames under `liteflow-rule-db/`.

- [ ] **Step 3: Add the new parent POM**

Create `liteflow-rule-db/pom.xml` with:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <parent>
        <artifactId>liteflow</artifactId>
        <groupId>com.yomahub</groupId>
        <version>${revision}</version>
        <relativePath>../pom.xml</relativePath>
    </parent>
    <modelVersion>4.0.0</modelVersion>
    <packaging>pom</packaging>
    <modules>
        <module>liteflow-rule-db-sql</module>
        <module>liteflow-rule-db-redis</module>
    </modules>

    <artifactId>liteflow-rule-db</artifactId>
    <name>${project.artifactId}</name>
</project>
```

- [ ] **Step 4: Point both child POMs at the new parent**

In both child POMs, replace only the parent artifactId:

```xml
<parent>
    <artifactId>liteflow-rule-db</artifactId>
    <groupId>com.yomahub</groupId>
    <version>${revision}</version>
    <relativePath>../pom.xml</relativePath>
</parent>
```

Keep each child artifactId and all dependencies unchanged.

- [ ] **Step 5: Remove Rule-DB children from the old parent**

Delete these two entries from `liteflow-rule-plugin/pom.xml`:

```xml
<module>liteflow-rule-db-sql</module>
<module>liteflow-rule-db-redis</module>
```

- [ ] **Step 6: Add the new parent to every applicable root profile**

In `pom.xml`, add the following immediately after `liteflow-rule-plugin` in the `compile-8-to-16`, `compile-17+` and `release-on-8` module lists:

```xml
<module>liteflow-rule-db</module>
```

- [ ] **Step 7: Verify the new Maven model and unchanged coordinates**

Run:

```bash
mvn -N help:evaluate -f liteflow-rule-db/pom.xml -Dexpression=project.modules -q -DforceStdout
mvn -N help:evaluate -f liteflow-rule-db/liteflow-rule-db-sql/pom.xml -Dexpression=project.parent.artifactId -q -DforceStdout
mvn -N help:evaluate -f liteflow-rule-db/liteflow-rule-db-sql/pom.xml -Dexpression=project.artifactId -q -DforceStdout
mvn -N help:evaluate -f liteflow-rule-db/liteflow-rule-db-redis/pom.xml -Dexpression=project.parent.artifactId -q -DforceStdout
mvn -N help:evaluate -f liteflow-rule-db/liteflow-rule-db-redis/pom.xml -Dexpression=project.artifactId -q -DforceStdout
```

Expected outputs include `[liteflow-rule-db-sql, liteflow-rule-db-redis]`, parent `liteflow-rule-db`, and unchanged child artifactIds `liteflow-rule-db-sql` and `liteflow-rule-db-redis`.

- [ ] **Step 8: Commit the Maven structure migration**

```bash
git add pom.xml liteflow-rule-plugin/pom.xml liteflow-rule-db
git commit -m "refactor: move Rule-DB into independent parent module"
```

### Task 2: Update repository documentation and historical plans

**Files:**
- Modify: `AGENTS.md`
- Modify: `CLAUDE.md`
- Modify: `docs/liteflow-rule-db-guide.md`
- Modify: `docs/superpowers/specs/2026-07-10-rule-db-plugin-design.md`
- Modify: `docs/superpowers/plans/2026-07-10-rule-db-core-and-sql.md`
- Modify: `docs/superpowers/plans/2026-07-10-rule-db-redis.md`
- Modify: any other tracked text file found by the old-path scan

- [ ] **Step 1: Capture all stale path and ownership references**

Run:

```bash
rg -n 'liteflow-rule-plugin/liteflow-rule-db-(sql|redis)|挂.*liteflow-rule-plugin|liteflow-rule-plugin.*聚合.*Rule-DB' \
  --glob '!**/target/**' \
  --glob '!docs/superpowers/plans/2026-07-11-rule-db-parent-module-relocation.md' .
```

Expected: matches in the Rule-DB guide and historical implementation plans before replacement.

- [ ] **Step 2: Update exact source paths in tracked documentation**

Apply these exact replacements in tracked text files:

```text
liteflow-rule-plugin/liteflow-rule-db-sql -> liteflow-rule-db/liteflow-rule-db-sql
liteflow-rule-plugin/liteflow-rule-db-redis -> liteflow-rule-db/liteflow-rule-db-redis
```

This includes Markdown links, Maven `-pl` examples, file lists and `git add` examples.

- [ ] **Step 3: Update module ownership descriptions**

In `AGENTS.md`, document `liteflow-rule-db` as a root-level parent containing the SQL and Redis Rule-DB implementations. In historical specs and plans, replace claims that either module is aggregated by `liteflow-rule-plugin` with the new `liteflow-rule-db` parent relationship. Preserve historical functional decisions and artifactId-only references.

- [ ] **Step 4: Prove stale references are gone**

Run:

```bash
! rg -n 'liteflow-rule-plugin/liteflow-rule-db-(sql|redis)|挂.*liteflow-rule-plugin|liteflow-rule-plugin.*聚合.*Rule-DB' \
  --glob '!**/target/**' \
  --glob '!docs/superpowers/plans/2026-07-11-rule-db-parent-module-relocation.md' .
```

Expected: exit code 0 with no output.

- [ ] **Step 5: Verify current guide links resolve**

Run:

```bash
test -f liteflow-rule-db/liteflow-rule-db-sql/src/main/resources/sql/ddl-mysql.sql
test -f liteflow-rule-db/liteflow-rule-db-redis/src/main/resources/lua/publish-chain.lua
```

Expected: both commands exit 0.

- [ ] **Step 6: Commit documentation updates**

```bash
git add AGENTS.md CLAUDE.md docs
git commit -m "docs: update Rule-DB module paths"
```

### Task 3: Build and regression verification

**Files:**
- Verify: `pom.xml`
- Verify: `liteflow-rule-db/pom.xml`
- Verify: `liteflow-rule-db/liteflow-rule-db-sql/pom.xml`
- Verify: `liteflow-rule-db/liteflow-rule-db-redis/pom.xml`
- Verify: `liteflow-testcase-el/liteflow-testcase-el-rule-db-sql-springboot/pom.xml`
- Verify: `liteflow-testcase-el/liteflow-testcase-el-rule-db-redis-springboot/pom.xml`

- [ ] **Step 1: Check patch integrity and module placement**

Run:

```bash
git diff --check HEAD~2..HEAD
test ! -e liteflow-rule-plugin/liteflow-rule-db-sql
test ! -e liteflow-rule-plugin/liteflow-rule-db-redis
test -f liteflow-rule-db/liteflow-rule-db-sql/pom.xml
test -f liteflow-rule-db/liteflow-rule-db-redis/pom.xml
```

Expected: all commands exit 0.

- [ ] **Step 2: Build both relocated production modules with reactor dependencies**

Run:

```bash
mvn package -DskipTests -pl liteflow-rule-db/liteflow-rule-db-sql,liteflow-rule-db/liteflow-rule-db-redis -am
```

Expected: `BUILD SUCCESS`, including `liteflow-rule-db`, `liteflow-rule-db-sql` and `liteflow-rule-db-redis` in the reactor summary.

- [ ] **Step 3: Run the SQL Rule-DB integration test module**

Run:

```bash
mvn test -pl liteflow-testcase-el/liteflow-testcase-el-rule-db-sql-springboot -am -DskipTests=false
```

Expected: `BUILD SUCCESS` with the Rule-DB SQL tests passing.

- [ ] **Step 4: Run the Redis Rule-DB integration test module**

Run:

```bash
mvn test -pl liteflow-testcase-el/liteflow-testcase-el-rule-db-redis-springboot -am -DskipTests=false
```

Expected: `BUILD SUCCESS` with the Rule-DB Redis tests passing. If the test requires an unavailable local Redis service, record the exact failure and still verify test dependency resolution with `mvn test-compile` for the same module.

- [ ] **Step 5: Confirm only structural and documentation changes remain**

Run:

```bash
git status --short
git diff HEAD~2 --stat
git diff --numstat HEAD~2 -- '*.java'
```

Expected: clean worktree after commits; every Java rename is reported with `0` added and `0` deleted lines, proving the move did not change Java source content.
