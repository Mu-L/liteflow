# Rule-DB 模式（core 运行时 + SQL 插件）实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 Rule-DB 模式的 core 运行时设施（RuleRepository SPI、常驻索引、有界缓存、变更同步、对账）与 `liteflow-rule-db-sql` 插件，达成"规则/脚本以存储为权威源、JVM 只做有界缓存、多节点最终一致收敛"。

**Architecture:** core 新增 `com.yomahub.liteflow.repository` 包（SPI + RuleDbRuntime 静态运行时），在 `LiteFlowChainELBuilder.buildUnCompileChain` 与 `FlowBus.compileScriptNode` 两个既有懒编译钩子处按需回源；`liteflow-rule-db-sql` 通过 ServiceLoader 注册 SPI 实现。规格见 `docs/superpowers/specs/2026-07-10-rule-db-plugin-design.md`。

**Tech Stack:** JDK 8 语法、hutool、Caffeine（core 既有依赖）、JDBC + H2（测试）、Spring Boot 2 starter 绑定。

**本计划是三个计划中的第 1 个**（计划 2：redis 插件；计划 3：Boot4/Solon 绑定 + metadata + 文档）。

## Global Constraints

- **JDK 8 语法**：core 与新插件均在 `compile-8-to-16` profile 下构建，禁用 var/List.of/switch 表达式等。
- **测试只放 `liteflow-testcase-el/` 下**（仓库强制约定），核心代码模块内不放任何测试。
- **运行测试必须加 `-DskipTests=false`**（根 pom surefire 默认 skip），**禁止改动这个默认值**。
- 版本占位符 `${revision}`（当前 2.16.1），新模块 pom 不写死版本。
- 新模块 `liteflow-rule-db-sql` 挂在 `liteflow-rule-plugin` 聚合 pom 下（根 pom 无需改：`liteflow-rule-plugin` 已在两个 compile profile 中）。
- 测试模块命名遵循仓库惯例：`liteflow-testcase-el-rule-db-core`（nospring 风格）、`liteflow-testcase-el-rule-db-sql-springboot`。
- 提交信息中文、conventional-commits 风格，结尾加 `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`。
- 每个 VO/配置类的"标准 getter/setter"指对全部字段生成常规 getter/setter（IDE 生成），不是可省略项。
- 编译验证命令：`mvn clean package -DskipTests -pl liteflow-core`（或对应模块）。

---

### Task 1: core SPI 类型、配置类与异常

**Files:**
- Create: `liteflow-core/src/main/java/com/yomahub/liteflow/repository/RuleRepository.java`
- Create: `liteflow-core/src/main/java/com/yomahub/liteflow/repository/RuleChangeListener.java`
- Create: `liteflow-core/src/main/java/com/yomahub/liteflow/repository/RuleRepositoryHolder.java`
- Create: `liteflow-core/src/main/java/com/yomahub/liteflow/repository/vo/ChainMeta.java`
- Create: `liteflow-core/src/main/java/com/yomahub/liteflow/repository/vo/ScriptMeta.java`
- Create: `liteflow-core/src/main/java/com/yomahub/liteflow/repository/vo/RuleManifest.java`
- Create: `liteflow-core/src/main/java/com/yomahub/liteflow/repository/vo/ChainRecord.java`
- Create: `liteflow-core/src/main/java/com/yomahub/liteflow/repository/vo/ScriptRecord.java`
- Create: `liteflow-core/src/main/java/com/yomahub/liteflow/repository/vo/ChangeRecord.java`
- Create: `liteflow-core/src/main/java/com/yomahub/liteflow/exception/ChainLoadException.java`
- Create: `liteflow-core/src/main/java/com/yomahub/liteflow/exception/SeqGapException.java`
- Create: `liteflow-core/src/main/java/com/yomahub/liteflow/property/RuleDbConfig.java`
- Modify: `liteflow-core/src/main/java/com/yomahub/liteflow/property/LiteflowConfig.java`（加 ruleDb 字段）

**Interfaces:**
- Produces: 后续所有任务依赖的类型。签名以本任务代码为准，特别是 `RuleRepository` 的 6 个方法、`RuleRepositoryHolder.get()/hasImplementation()/reset()`、`RuleDbConfig` 的字段名。

- [ ] **Step 1: 写 SPI 接口与 VO**

`RuleRepository.java`：

```java
package com.yomahub.liteflow.repository;

import com.yomahub.liteflow.repository.vo.ChainRecord;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import com.yomahub.liteflow.repository.vo.RuleManifest;
import com.yomahub.liteflow.repository.vo.ScriptRecord;

import java.util.List;

/**
 * Rule-DB 模式的规则权威源 SPI。
 * 实现类通过 ServiceLoader 注册（META-INF/services/com.yomahub.liteflow.repository.RuleRepository），
 * 必须提供无参构造器，连接等初始化在首次方法调用时基于 LiteflowConfigGetter.get().getRuleDb() 懒执行。
 *
 * @author Bryan.Zhang
 * @since 2.16.1
 */
public interface RuleRepository {

    /** 清单：全部 chain/script 的 id+version+md5+脚本元数据（不含内容） */
    RuleManifest fetchManifest();

    /** 按 id 取 chain 内容；不存在返回 null */
    ChainRecord fetchChain(String chainId);

    /** 按 id 取脚本内容；不存在返回 null */
    ScriptRecord fetchScript(String nodeId);

    /** 当前最大变更序号；无变更记录时返回 0 */
    long fetchLatestSeq();

    /** 取 seq 之后的增量变更（升序）；发现 seq 已断档（变更日志被清理）时抛 SeqGapException */
    List<ChangeRecord> fetchChangesSince(long seq);

    /** 可选推送通道（Redis 实现，SQL 空实现） */
    default void subscribe(RuleChangeListener listener) {
    }

    /** 释放连接资源 */
    default void close() {
    }
}
```

`RuleChangeListener.java`：

```java
package com.yomahub.liteflow.repository;

import com.yomahub.liteflow.repository.vo.ChangeRecord;

import java.util.List;

public interface RuleChangeListener {

    void onChanges(List<ChangeRecord> changes);
}
```

VO（包 `com.yomahub.liteflow.repository.vo`，每个类补全标准 getter/setter）：

```java
public class ChainMeta {
    private String chainId;
    private long version;
    private String md5;

    public ChainMeta() {}
    public ChainMeta(String chainId, long version, String md5) {
        this.chainId = chainId; this.version = version; this.md5 = md5;
    }
}

public class ScriptMeta {
    private String nodeId;
    private long version;
    private String md5;
    private String type;      // NodeTypeEnum code：script/boolean_script/switch_script/...
    private String language;  // 可空
    private String name;      // 可空

    public ScriptMeta() {}
    public ScriptMeta(String nodeId, long version, String md5, String type, String language, String name) {
        this.nodeId = nodeId; this.version = version; this.md5 = md5;
        this.type = type; this.language = language; this.name = name;
    }
}

public class RuleManifest {
    private List<ChainMeta> chains = new ArrayList<>();
    private List<ScriptMeta> scripts = new ArrayList<>();
    private long latestSeq;
}

public class ChainRecord {
    private String chainId;
    private String el;
    private String route;      // 可空
    private String namespace;  // 可空
    private long version;
    private String md5;
    private boolean enable = true;
}

public class ScriptRecord {
    private String nodeId;
    private String script;
    private String name;       // 可空
    private String type;       // NodeTypeEnum code
    private String language;   // 可空
    private long version;
    private String md5;
    private boolean enable = true;
}

public class ChangeRecord {
    public enum TargetType { CHAIN, SCRIPT }
    public enum Op { UPSERT, DELETE }

    private long seq;
    private TargetType targetType;
    private String targetId;
    private Op op;
    private long version;

    public ChangeRecord() {}
    public ChangeRecord(long seq, TargetType targetType, String targetId, Op op, long version) {
        this.seq = seq; this.targetType = targetType; this.targetId = targetId;
        this.op = op; this.version = version;
    }
}
```

- [ ] **Step 2: 写异常与 Holder**

`ChainLoadException.java` / `SeqGapException.java`（仿 `com.yomahub.liteflow.exception` 包内既有异常的写法，继承同一个基类——打开该包任一异常类确认基类与构造器形态后照写）：

```java
package com.yomahub.liteflow.exception;

/** Rule-DB 模式下回源加载失败（区别于 ChainNotFoundException：规则存在但取不回来） */
public class ChainLoadException extends LiteFlowException {
    public ChainLoadException(String message) {
        super(message);
    }
}

public class SeqGapException extends LiteFlowException {
    public SeqGapException(String message) {
        super(message);
    }
}
```

`RuleRepositoryHolder.java`：

```java
package com.yomahub.liteflow.repository;

import com.yomahub.liteflow.exception.ConfigErrorException;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

public class RuleRepositoryHolder {

    private static volatile RuleRepository repository;

    private static volatile boolean resolved = false;

    public static synchronized RuleRepository get() {
        if (!resolved) {
            List<RuleRepository> impls = new ArrayList<>();
            for (RuleRepository r : ServiceLoader.load(RuleRepository.class)) {
                impls.add(r);
            }
            if (impls.size() > 1) {
                throw new ConfigErrorException("multiple RuleRepository implementations found on classpath, keep only one of liteflow-rule-db-sql / liteflow-rule-db-redis");
            }
            repository = impls.isEmpty() ? null : impls.get(0);
            resolved = true;
        }
        return repository;
    }

    public static boolean hasImplementation() {
        return get() != null;
    }

    /** 供测试与 destroy 重置 ServiceLoader 解析结果 */
    public static synchronized void reset() {
        repository = null;
        resolved = false;
    }
}
```

- [ ] **Step 3: 写 RuleDbConfig 并挂到 LiteflowConfig**

`RuleDbConfig.java`（补全标准 getter/setter）：

```java
package com.yomahub.liteflow.property;

/** Rule-DB 模式统一配置（liteflow.rule-db.*），SQL/Redis 字段取并集，插件各取所需 */
public class RuleDbConfig {

    private Boolean enabled = Boolean.TRUE;
    private String applicationName;
    private Integer cacheCapacity = 500;
    private Integer seqPollSeconds;            // null=插件默认（SQL:3 / Redis:30）
    private Integer reconcileSeconds = 60;
    private String preloadChainIds;            // 逗号分隔
    private Integer fetchRetryTimes = 3;

    // ---- SQL ----
    private String url;
    private String username;
    private String password;
    private String driverClassName;            // 空则由 url 推断
    private String datasourceBeanName;         // 空则自动查找容器 DataSource
    private String tablePrefix = "lf_";
    private Boolean autoInitTable = Boolean.FALSE;

    // ---- Redis ----
    private String address;                    // 多地址逗号分隔
    private String masterName;                 // 配置即哨兵模式
    private Integer database = 0;
    private String keyPrefix = "lf";
    private String redissonBeanName;
}
```

`LiteflowConfig.java` 新增字段（放在 chainCacheCapacity 字段附近，getter/setter 仿相邻字段风格）：

```java
	//Rule-DB模式配置
	private RuleDbConfig ruleDb;

	public RuleDbConfig getRuleDb() {
		return ruleDb;
	}

	public void setRuleDb(RuleDbConfig ruleDb) {
		this.ruleDb = ruleDb;
	}
```

- [ ] **Step 4: 编译验证**

Run: `mvn clean package -DskipTests -pl liteflow-core`
Expected: BUILD SUCCESS

- [ ] **Step 5: Commit**

```bash
git add liteflow-core/src/main/java/com/yomahub/liteflow/repository liteflow-core/src/main/java/com/yomahub/liteflow/exception/ChainLoadException.java liteflow-core/src/main/java/com/yomahub/liteflow/exception/SeqGapException.java liteflow-core/src/main/java/com/yomahub/liteflow/property/RuleDbConfig.java liteflow-core/src/main/java/com/yomahub/liteflow/property/LiteflowConfig.java
git commit -m "feat(core): Rule-DB 模式 SPI 类型、配置类与异常

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: 核心测试基建（InMemory 权威源）+ 首个失败测试

**Files:**
- Create: `liteflow-testcase-el/liteflow-testcase-el-rule-db-core/pom.xml`
- Modify: `liteflow-testcase-el/pom.xml`（modules 里加 `<module>liteflow-testcase-el-rule-db-core</module>`）
- Create: `liteflow-testcase-el/liteflow-testcase-el-rule-db-core/src/test/java/com/yomahub/liteflow/test/ruledb/InMemoryRuleRepository.java`
- Create: `liteflow-testcase-el/liteflow-testcase-el-rule-db-core/src/test/resources/META-INF/services/com.yomahub.liteflow.repository.RuleRepository`
- Create: `liteflow-testcase-el/liteflow-testcase-el-rule-db-core/src/test/java/com/yomahub/liteflow/test/ruledb/BaseRuleDbTest.java`
- Create: `liteflow-testcase-el/liteflow-testcase-el-rule-db-core/src/test/java/com/yomahub/liteflow/test/ruledb/cmp/ACmp.java`（同款 BCmp/CCmp）
- Create: `liteflow-testcase-el/liteflow-testcase-el-rule-db-core/src/test/java/com/yomahub/liteflow/test/ruledb/RuleDbStartupTest.java`
- Create: `liteflow-testcase-el/liteflow-testcase-el-rule-db-core/src/test/resources/logback.xml`（从 liteflow-testcase-el-nospring 同名文件原样复制）

**Interfaces:**
- Consumes: Task 1 全部类型。
- Produces: `InMemoryRuleRepository` 的静态测试操作面：`CHAINS`/`SCRIPTS`/`SEQ`/`CHANGES`/`FETCH_CHAIN_COUNT`/`FETCH_SCRIPT_COUNT`/`DOWN`/`MIN_SEQ`、`putChain(chainId, el)`（不记变更）、`publishChain(chainId, el)`、`publishScript(nodeId, script, type, language)`、`deleteChain(chainId)`、`reset()`；`BaseRuleDbTest` 的 `@AfterEach cleanup()` 与 `buildExecutor(RuleDbConfig)`、`waitUntil(supplier, timeoutMs)`。后续 Task 3~8 的所有测试都基于这套基建。

- [ ] **Step 1: 建测试模块 pom 并挂到聚合 pom**

`liteflow-testcase-el/liteflow-testcase-el-rule-db-core/pom.xml`：

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <parent>
        <artifactId>liteflow-testcase-el</artifactId>
        <groupId>com.yomahub</groupId>
        <version>${revision}</version>
        <relativePath>../pom.xml</relativePath>
    </parent>
    <modelVersion>4.0.0</modelVersion>
    <artifactId>liteflow-testcase-el-rule-db-core</artifactId>
    <dependencies>
        <dependency>
            <groupId>com.yomahub</groupId>
            <artifactId>liteflow-core</artifactId>
            <version>${revision}</version>
        </dependency>
        <dependency>
            <groupId>com.yomahub</groupId>
            <artifactId>liteflow-script-groovy</artifactId>
            <version>${revision}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>ch.qos.logback</groupId>
            <artifactId>logback-classic</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
        </dependency>
    </dependencies>
</project>
```

`liteflow-testcase-el/pom.xml` 的 `<modules>` 末尾（第 47 行 `liteflow-testcase-el-script-javaxpro-springboot` 之后）加：

```xml
        <module>liteflow-testcase-el-rule-db-core</module>
```

- [ ] **Step 2: 写 InMemoryRuleRepository 与 ServiceLoader 注册**

```java
package com.yomahub.liteflow.test.ruledb;

import cn.hutool.crypto.SecureUtil;
import com.yomahub.liteflow.exception.SeqGapException;
import com.yomahub.liteflow.repository.RuleChangeListener;
import com.yomahub.liteflow.repository.RuleRepository;
import com.yomahub.liteflow.repository.vo.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** 测试用内存权威源。静态状态便于测试内"另一个节点发布"式操作 */
public class InMemoryRuleRepository implements RuleRepository {

    public static final Map<String, ChainRecord> CHAINS = new ConcurrentHashMap<>();
    public static final Map<String, ScriptRecord> SCRIPTS = new ConcurrentHashMap<>();
    public static final List<ChangeRecord> CHANGES = new CopyOnWriteArrayList<>();
    public static final AtomicLong SEQ = new AtomicLong(0);
    public static final AtomicInteger FETCH_CHAIN_COUNT = new AtomicInteger(0);
    public static final AtomicInteger FETCH_SCRIPT_COUNT = new AtomicInteger(0);
    public static volatile boolean DOWN = false;
    public static volatile long MIN_SEQ = 0;

    public static void reset() {
        CHAINS.clear(); SCRIPTS.clear(); CHANGES.clear();
        SEQ.set(0); FETCH_CHAIN_COUNT.set(0); FETCH_SCRIPT_COUNT.set(0);
        DOWN = false; MIN_SEQ = 0;
    }

    /** 只放数据不记变更（模拟绕过发布规范的脏写/初始数据） */
    public static void putChain(String chainId, String el) {
        ChainRecord old = CHAINS.get(chainId);
        long version = old == null ? 1 : old.getVersion() + 1;
        ChainRecord r = new ChainRecord();
        r.setChainId(chainId); r.setEl(el); r.setVersion(version);
        r.setMd5(SecureUtil.md5(el)); r.setEnable(true);
        CHAINS.put(chainId, r);
    }

    public static void putScript(String nodeId, String script, String type, String language) {
        ScriptRecord old = SCRIPTS.get(nodeId);
        long version = old == null ? 1 : old.getVersion() + 1;
        ScriptRecord r = new ScriptRecord();
        r.setNodeId(nodeId); r.setScript(script); r.setType(type); r.setLanguage(language);
        r.setVersion(version); r.setMd5(SecureUtil.md5(script)); r.setEnable(true);
        SCRIPTS.put(nodeId, r);
    }

    /** 规范发布：内容 + 版本 + 变更序号 */
    public static void publishChain(String chainId, String el) {
        putChain(chainId, el);
        CHANGES.add(new ChangeRecord(SEQ.incrementAndGet(), ChangeRecord.TargetType.CHAIN,
                chainId, ChangeRecord.Op.UPSERT, CHAINS.get(chainId).getVersion()));
    }

    public static void publishScript(String nodeId, String script, String type, String language) {
        putScript(nodeId, script, type, language);
        CHANGES.add(new ChangeRecord(SEQ.incrementAndGet(), ChangeRecord.TargetType.SCRIPT,
                nodeId, ChangeRecord.Op.UPSERT, SCRIPTS.get(nodeId).getVersion()));
    }

    public static void deleteChain(String chainId) {
        ChainRecord old = CHAINS.remove(chainId);
        long version = old == null ? 0 : old.getVersion();
        CHANGES.add(new ChangeRecord(SEQ.incrementAndGet(), ChangeRecord.TargetType.CHAIN,
                chainId, ChangeRecord.Op.DELETE, version));
    }

    private void checkDown() {
        if (DOWN) {
            throw new RuntimeException("in-memory rule repository is down");
        }
    }

    @Override
    public RuleManifest fetchManifest() {
        checkDown();
        RuleManifest m = new RuleManifest();
        List<ChainMeta> chains = new ArrayList<>();
        for (ChainRecord r : CHAINS.values()) {
            if (r.isEnable()) {
                chains.add(new ChainMeta(r.getChainId(), r.getVersion(), r.getMd5()));
            }
        }
        List<ScriptMeta> scripts = new ArrayList<>();
        for (ScriptRecord r : SCRIPTS.values()) {
            if (r.isEnable()) {
                scripts.add(new ScriptMeta(r.getNodeId(), r.getVersion(), r.getMd5(),
                        r.getType(), r.getLanguage(), r.getName()));
            }
        }
        m.setChains(chains); m.setScripts(scripts); m.setLatestSeq(SEQ.get());
        return m;
    }

    @Override
    public ChainRecord fetchChain(String chainId) {
        checkDown();
        FETCH_CHAIN_COUNT.incrementAndGet();
        return CHAINS.get(chainId);
    }

    @Override
    public ScriptRecord fetchScript(String nodeId) {
        checkDown();
        FETCH_SCRIPT_COUNT.incrementAndGet();
        return SCRIPTS.get(nodeId);
    }

    @Override
    public long fetchLatestSeq() {
        checkDown();
        return SEQ.get();
    }

    @Override
    public List<ChangeRecord> fetchChangesSince(long seq) {
        checkDown();
        if (seq + 1 < MIN_SEQ) {
            throw new SeqGapException("change log has been cleaned, since=" + seq + " min=" + MIN_SEQ);
        }
        List<ChangeRecord> result = new ArrayList<>();
        for (ChangeRecord c : CHANGES) {
            if (c.getSeq() > seq) {
                result.add(c);
            }
        }
        return result;
    }
}
```

`src/test/resources/META-INF/services/com.yomahub.liteflow.repository.RuleRepository` 内容一行：

```
com.yomahub.liteflow.test.ruledb.InMemoryRuleRepository
```

- [ ] **Step 3: 写 BaseRuleDbTest 与组件**

```java
package com.yomahub.liteflow.test.ruledb;

import com.yomahub.liteflow.builder.LiteFlowNodeBuilder;
import com.yomahub.liteflow.core.FlowExecutor;
import com.yomahub.liteflow.core.FlowExecutorHolder;
import com.yomahub.liteflow.flow.FlowBus;
import com.yomahub.liteflow.property.LiteflowConfig;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.repository.RuleDbRuntime;
import com.yomahub.liteflow.repository.RuleRepositoryHolder;
import com.yomahub.liteflow.test.ruledb.cmp.ACmp;
import com.yomahub.liteflow.test.ruledb.cmp.BCmp;
import com.yomahub.liteflow.test.ruledb.cmp.CCmp;
import org.junit.jupiter.api.AfterEach;

import java.util.function.Supplier;

public abstract class BaseRuleDbTest {

    @AfterEach
    public void cleanup() {
        RuleDbRuntime.destroy();
        RuleRepositoryHolder.reset();
        FlowBus.cleanCache();
        FlowBus.clearStat();
        InMemoryRuleRepository.reset();
    }

    /** 注册普通 Java 组件（rule-db 只纳管 EL 与脚本，Java 组件仍在 JVM 内） */
    protected void registerCommonCmp() {
        LiteFlowNodeBuilder.createCommonNode().setId("a").setClazz(ACmp.class).build();
        LiteFlowNodeBuilder.createCommonNode().setId("b").setClazz(BCmp.class).build();
        LiteFlowNodeBuilder.createCommonNode().setId("c").setClazz(CCmp.class).build();
    }

    protected FlowExecutor buildExecutor(RuleDbConfig ruleDb) {
        LiteflowConfig config = new LiteflowConfig();
        config.setRuleDb(ruleDb);
        return FlowExecutorHolder.loadInstance(config);
    }

    protected void waitUntil(Supplier<Boolean> condition, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (Boolean.TRUE.equals(condition.get())) {
                return;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        throw new AssertionError("condition not met within " + timeoutMs + "ms");
    }
}
```

注意：`FlowExecutorHolder.loadInstance(config)` 可能持有单例，第二个测试类拿到旧实例。参照 `liteflow-testcase-el-nospring` 里 BaseTest 的做法处理（若其有 `FlowExecutorHolder` 清理手段则照抄；若没有，各测试用 `flowExecutor.reloadRule()` 不适用于 rule-db，改为在 `buildExecutor` 前调用 `FlowBus.clearStat()` 并接受单例复用——executor 是无状态入口，配置经 `LiteflowConfigGetter` 全局获取，重建 `LiteflowConfig` 即可生效）。

组件（BCmp/CCmp 同款改名与输出）：

```java
package com.yomahub.liteflow.test.ruledb.cmp;

import com.yomahub.liteflow.core.NodeComponent;

public class ACmp extends NodeComponent {

    @Override
    public void process() throws Exception {
        System.out.println("ACmp executed!");
    }
}
```

- [ ] **Step 4: 写首个失败测试（启动只建索引与影子，不回源内容）**

`RuleDbStartupTest.java`：

```java
package com.yomahub.liteflow.test.ruledb;

import com.yomahub.liteflow.enums.NodeTypeEnum;
import com.yomahub.liteflow.flow.FlowBus;
import com.yomahub.liteflow.flow.element.Chain;
import com.yomahub.liteflow.flow.element.Node;
import com.yomahub.liteflow.property.RuleDbConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RuleDbStartupTest extends BaseRuleDbTest {

    @Test
    public void testStartupBuildsShadowIndexWithoutFetchingContent() {
        InMemoryRuleRepository.putChain("chain1", "THEN(a, b)");
        InMemoryRuleRepository.putScript("s1", "println('s1 run')", "script", "groovy");
        registerCommonCmp();

        buildExecutor(new RuleDbConfig());

        // chain 影子已注册但未编译、无 EL
        Chain chain = FlowBus.getChain("chain1");
        Assertions.assertNotNull(chain);
        Assertions.assertFalse(chain.isCompiled());
        Assertions.assertNull(chain.getEl());

        // 脚本影子已注册：有元数据、无源码
        Node node = FlowBus.getNode("s1");
        Assertions.assertNotNull(node);
        Assertions.assertEquals(NodeTypeEnum.SCRIPT, node.getType());
        Assertions.assertEquals("groovy", node.getLanguage());
        Assertions.assertNull(node.getScript());

        // 未发生任何内容回源
        Assertions.assertEquals(0, InMemoryRuleRepository.FETCH_CHAIN_COUNT.get());
        Assertions.assertEquals(0, InMemoryRuleRepository.FETCH_SCRIPT_COUNT.get());
    }
}
```

- [ ] **Step 5: 运行测试确认失败**

Run: `mvn test -DskipTests=false -pl liteflow-testcase-el/liteflow-testcase-el-rule-db-core -Dtest=RuleDbStartupTest`
Expected: FAIL（编译错误：`RuleDbRuntime` 尚不存在——这是预期失败形态；若想先跑通编译，可临时注释 BaseRuleDbTest 中对 RuleDbRuntime 的引用，Task 3 实现后恢复）

- [ ] **Step 6: Commit**

```bash
git add liteflow-testcase-el/pom.xml liteflow-testcase-el/liteflow-testcase-el-rule-db-core
git commit -m "test(rule-db): 核心测试基建（InMemory 权威源）与启动影子注册测试

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: RuleDbRuntime —— 启动建索引 + 懒回源接线

**Files:**
- Create: `liteflow-core/src/main/java/com/yomahub/liteflow/repository/RuleDbRuntime.java`
- Modify: `liteflow-core/src/main/java/com/yomahub/liteflow/core/FlowExecutor.java`（init 分流 + 互斥检查）
- Modify: `liteflow-core/src/main/java/com/yomahub/liteflow/builder/el/LiteFlowChainELBuilder.java:336-341`（buildUnCompileChain 回源钩子）
- Modify: `liteflow-core/src/main/java/com/yomahub/liteflow/flow/FlowBus.java:273`（compileScriptNode 回源钩子）
- Modify（测试基建）: `liteflow-testcase-el/liteflow-testcase-el-rule-db-core/.../BaseRuleDbTest.java`（恢复 Task 2 注释的 RuleDbRuntime 引用，并按 nospring BaseTest 拓宽 cleanup）
- Test: `liteflow-testcase-el-rule-db-core/.../RuleDbLazyLoadTest.java`

**Interfaces:**
- Consumes: Task 1 的 `RuleRepository`/`RuleRepositoryHolder`/VO/`RuleDbConfig`；Task 2 的测试基建。
- Produces:
  - `RuleDbRuntime.isActive()` : boolean —— classpath 有实现且 `ruleDb.enabled != false`
  - `RuleDbRuntime.init()` : void —— 建索引、注册影子、记录 lastAppliedSeq、预热
  - `RuleDbRuntime.destroy()` : void —— 停后台线程、close repository、清索引状态
  - `RuleDbRuntime.ensureChainLoaded(String chainId)` : void —— 影子/失效时回源编译入口（buildUnCompileChain 调用）
  - `RuleDbRuntime.ensureScriptLoaded(String nodeId)` : void —— 影子/失效时回源编译入口（compileScriptNode 调用）

- [ ] **Step 1: 写 RuleDbRuntime（启动 + 回源部分；后台同步/缓存登记留到 Task 4/5）**

`RuleDbRuntime.java`：

```java
package com.yomahub.liteflow.repository;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.builder.el.LiteFlowChainELBuilder;
import com.yomahub.liteflow.enums.NodeTypeEnum;
import com.yomahub.liteflow.exception.ChainLoadException;
import com.yomahub.liteflow.flow.FlowBus;
import com.yomahub.liteflow.flow.element.Chain;
import com.yomahub.liteflow.flow.element.Node;
import com.yomahub.liteflow.log.LFLog;
import com.yomahub.liteflow.log.LFLoggerManager;
import com.yomahub.liteflow.property.LiteflowConfig;
import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.repository.vo.ChainMeta;
import com.yomahub.liteflow.repository.vo.ChainRecord;
import com.yomahub.liteflow.repository.vo.RuleManifest;
import com.yomahub.liteflow.repository.vo.ScriptMeta;
import com.yomahub.liteflow.repository.vo.ScriptRecord;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Rule-DB 模式运行时：常驻版本戳索引 + 懒回源。后台变更同步/对账在 Task 5 补入，
 * 有界缓存在 Task 4 补入。
 *
 * @author Bryan.Zhang
 * @since 2.16.1
 */
public class RuleDbRuntime {

    private static final LFLog LOG = LFLoggerManager.getLogger(RuleDbRuntime.class);

    /** chainId -> 权威版本戳（常驻索引） */
    private static final Map<String, Long> CHAIN_VERSION_INDEX = new ConcurrentHashMap<>();

    /** nodeId -> 权威版本戳（常驻索引） */
    private static final Map<String, Long> SCRIPT_VERSION_INDEX = new ConcurrentHashMap<>();

    /** 已回源编译进 FlowBus 的 chain 当前版本（缓存态；Task 4 起由 RuleDbCache 协同） */
    private static final Map<String, Long> CHAIN_CACHED_VERSION = new ConcurrentHashMap<>();

    private static final Map<String, Long> SCRIPT_CACHED_VERSION = new ConcurrentHashMap<>();

    static final AtomicLong LAST_APPLIED_SEQ = new AtomicLong(0);

    private static volatile boolean initialized = false;

    public static boolean isActive() {
        if (!RuleRepositoryHolder.hasImplementation()) {
            return false;
        }
        LiteflowConfig config = LiteflowConfigGetter.get();
        RuleDbConfig ruleDb = config.getRuleDb();
        // 未显式配置 ruleDb 但 classpath 有实现，也视为激活（零配置理念）
        return ruleDb == null || ruleDb.getEnabled() == null || Boolean.TRUE.equals(ruleDb.getEnabled());
    }

    public static synchronized void init() {
        if (initialized) {
            return;
        }
        RuleRepository repo = RuleRepositoryHolder.get();
        RuleManifest manifest = repo.fetchManifest();

        // 注册 chain 影子
        if (CollUtil.isNotEmpty(manifest.getChains())) {
            for (ChainMeta cm : manifest.getChains()) {
                CHAIN_VERSION_INDEX.put(cm.getChainId(), cm.getVersion());
                FlowBus.addChain(cm.getChainId()); // 影子 Chain：只有 id，isCompiled=false
            }
        }
        // 注册 script 影子
        if (CollUtil.isNotEmpty(manifest.getScripts())) {
            for (ScriptMeta sm : manifest.getScripts()) {
                SCRIPT_VERSION_INDEX.put(sm.getNodeId(), sm.getVersion());
                registerShadowScript(sm);
            }
        }
        LAST_APPLIED_SEQ.set(manifest.getLatestSeq());
        initialized = true;

        // 预热
        preload();
    }

    private static void preload() {
        RuleDbConfig ruleDb = LiteflowConfigGetter.get().getRuleDb();
        if (ruleDb == null || StrUtil.isBlank(ruleDb.getPreloadChainIds())) {
            return;
        }
        for (String chainId : ruleDb.getPreloadChainIds().split(",")) {
            String trimmed = chainId.trim();
            if (StrUtil.isNotBlank(trimmed) && CHAIN_VERSION_INDEX.containsKey(trimmed)) {
                try {
                    ensureChainLoaded(trimmed);
                    LiteFlowChainELBuilder.buildUnCompileChain(FlowBus.getChain(trimmed));
                } catch (Exception e) {
                    LOG.warn("preload chain[{}] failed: {}", trimmed, e.getMessage());
                }
            }
        }
    }

    /**
     * 注册脚本影子 Node：有元数据（type/language/name），无脚本源码，isCompiled=false。
     * 直接 new Node 并放入 nodeMap，进入未编译态。
     */
    private static void registerShadowScript(ScriptMeta sm) {
        NodeTypeEnum type = NodeTypeEnum.getEnumByCode(sm.getType());
        Node node = new Node(sm.getNodeId(), sm.getName(), type, null, sm.getLanguage());
        node.setCompiled(false);
        FlowBus.getNodeMap().put(sm.getNodeId(), node);
    }

    /** buildUnCompileChain 回源钩子：影子或版本失效时，拉内容填 EL，交由后续既有编译逻辑 */
    public static void ensureChainLoaded(String chainId) {
        Long authVersion = CHAIN_VERSION_INDEX.get(chainId);
        if (authVersion == null) {
            return; // 非 rule-db 管理的 chain（如手动 build），不干预
        }
        Chain chain = FlowBus.getChain(chainId);
        Long cachedVersion = CHAIN_CACHED_VERSION.get(chainId);
        if (chain != null && StrUtil.isNotBlank(chain.getEl())
                && authVersion.equals(cachedVersion)) {
            return; // EL 已在手且版本一致
        }
        ChainRecord record = fetchChainWithRetry(chainId);
        if (record == null || !record.isEnable()) {
            throw new ChainLoadException(StrUtil.format("chain[{}] not found or disabled in rule repository", chainId));
        }
        chain.setEl(record.getEl());
        chain.setRouteEl(record.getRoute());
        if (StrUtil.isNotBlank(record.getNamespace())) {
            chain.setNamespace(record.getNamespace());
        }
        chain.setCompiled(false);
        CHAIN_CACHED_VERSION.put(chainId, record.getVersion());
        CHAIN_VERSION_INDEX.put(chainId, record.getVersion());
    }

    /** compileScriptNode 回源钩子：脚本影子/失效时拉源码填入 node */
    public static void ensureScriptLoaded(String nodeId) {
        Long authVersion = SCRIPT_VERSION_INDEX.get(nodeId);
        if (authVersion == null) {
            return;
        }
        Node node = FlowBus.getNode(nodeId);
        Long cachedVersion = SCRIPT_CACHED_VERSION.get(nodeId);
        if (node != null && StrUtil.isNotBlank(node.getScript())
                && authVersion.equals(cachedVersion)) {
            return;
        }
        ScriptRecord record = fetchScriptWithRetry(nodeId);
        if (record == null || !record.isEnable()) {
            throw new ChainLoadException(StrUtil.format("script node[{}] not found or disabled in rule repository", nodeId));
        }
        node.setScript(record.getScript());
        node.setLanguage(record.getLanguage());
        SCRIPT_CACHED_VERSION.put(nodeId, record.getVersion());
        SCRIPT_VERSION_INDEX.put(nodeId, record.getVersion());
    }

    private static ChainRecord fetchChainWithRetry(String chainId) {
        int retry = retryTimes();
        RuntimeException last = null;
        for (int i = 0; i <= retry; i++) {
            try {
                return RuleRepositoryHolder.get().fetchChain(chainId);
            } catch (RuntimeException e) {
                last = e;
            }
        }
        throw new ChainLoadException(StrUtil.format("fetch chain[{}] failed after {} retries: {}",
                chainId, retry, last == null ? StrUtil.EMPTY : last.getMessage()));
    }

    private static ScriptRecord fetchScriptWithRetry(String nodeId) {
        int retry = retryTimes();
        RuntimeException last = null;
        for (int i = 0; i <= retry; i++) {
            try {
                return RuleRepositoryHolder.get().fetchScript(nodeId);
            } catch (RuntimeException e) {
                last = e;
            }
        }
        throw new ChainLoadException(StrUtil.format("fetch script[{}] failed after {} retries: {}",
                nodeId, retry, last == null ? StrUtil.EMPTY : last.getMessage()));
    }

    private static int retryTimes() {
        RuleDbConfig ruleDb = LiteflowConfigGetter.get().getRuleDb();
        return ruleDb == null || ruleDb.getFetchRetryTimes() == null ? 3 : ruleDb.getFetchRetryTimes();
    }

    // ---- 索引/缓存态操作，供 Task 4/5 的 Cache/SyncManager 使用 ----

    public static Long getChainVersion(String chainId) {
        return CHAIN_VERSION_INDEX.get(chainId);
    }

    public static void putChainVersion(String chainId, long version) {
        CHAIN_VERSION_INDEX.put(chainId, version);
    }

    public static void putScriptVersion(String nodeId, long version) {
        SCRIPT_VERSION_INDEX.put(nodeId, version);
    }

    public static Map<String, Long> chainVersionIndex() {
        return CHAIN_VERSION_INDEX;
    }

    public static Map<String, Long> scriptVersionIndex() {
        return SCRIPT_VERSION_INDEX;
    }

    static void onChainEvicted(String chainId) {
        CHAIN_CACHED_VERSION.remove(chainId);
    }

    static void onScriptEvicted(String nodeId) {
        SCRIPT_CACHED_VERSION.remove(nodeId);
    }

    /** Task 5 的 applyChange/reconcile 使用的影子注册（清单新增 chain 时） */
    static void registerShadowChain(String chainId) {
        CHAIN_VERSION_INDEX.put(chainId, 0L);
        FlowBus.addChain(chainId);
    }

    static void registerShadowScript(ScriptMeta sm) {
        SCRIPT_VERSION_INDEX.put(sm.getNodeId(), sm.getVersion());
        registerShadowScript(sm);
    }

    /** 缓存态失效（Task 5 分级刷新用） */
    static void invalidateChainCache(String chainId) {
        CHAIN_CACHED_VERSION.remove(chainId);
        Chain chain = FlowBus.getChain(chainId);
        if (chain != null) {
            chain.setCompiled(false);
            chain.setConditionList(null);
            chain.setEl(null);
        }
    }

    static void invalidateScriptCache(String nodeId) {
        SCRIPT_CACHED_VERSION.remove(nodeId);
        Node node = FlowBus.getNode(nodeId);
        if (node != null) {
            node.setScript(null);
            node.setCompiled(false);
        }
    }

    public static synchronized void destroy() {
        CHAIN_VERSION_INDEX.clear();
        SCRIPT_VERSION_INDEX.clear();
        CHAIN_CACHED_VERSION.clear();
        SCRIPT_CACHED_VERSION.clear();
        LAST_APPLIED_SEQ.set(0);
        initialized = false;
        RuleRepository repo = RuleRepositoryHolder.get();
        if (repo != null) {
            try {
                repo.close();
            } catch (Exception ignored) {
            }
        }
    }
}
```

注意点：
- `new Node(nodeId, name, type, script, language)` 五参构造器已存在（FlowBus.addScriptNode 用法）。
- `Chain.setRouteEl`/`setNamespace`/`setEl`/`setConditionList` 均为既有 setter（Task 4/5 会用到 setConditionList）。
- Task 3 阶段 `recordCompiledChain`（缓存登记）尚未引入，那是 Task 4 的职责；Task 3 只保证"回源填 EL → 既有编译跑通 → 测试绿"。

- [ ] **Step 2: 在 FlowExecutor.init 分流并加互斥检查**

`FlowExecutor.java` init 方法，在 chainCache 初始化块（`if (isStart && liteflowConfig.getChainCacheEnabled()) {...}`，约第 118-121 行）之后、`String ruleSource = liteflowConfig.getRuleSource();`（第 123 行）之前插入：

```java
		// Rule-DB 模式：classpath 存在 RuleRepository 实现且启用
		if (com.yomahub.liteflow.repository.RuleDbRuntime.isActive()) {
			if (StrUtil.isNotBlank(liteflowConfig.getRuleSource())) {
				throw new ConfigErrorException("rule-source and rule-db mode cannot be used together, please remove one of them");
			}
			com.yomahub.liteflow.repository.RuleDbRuntime.init();
			return;
		}
```

> rule-db 有自己的缓存治理，不复用 ChainCacheLifeCycle。`ConfigErrorException` 已存在于 `com.yomahub.liteflow.exception`。

- [ ] **Step 3: buildUnCompileChain 回源钩子**

`LiteFlowChainELBuilder.java` 的 `buildUnCompileChain`（约第 336-341 行）改为先回源：

```java
	public static void buildUnCompileChain(Chain chain){
		// Rule-DB 模式：影子/失效 chain 先回源填 EL
		if (com.yomahub.liteflow.repository.RuleDbRuntime.isActive()){
			com.yomahub.liteflow.repository.RuleDbRuntime.ensureChainLoaded(chain.getChainId());
		}
		if (StrUtil.isBlank(chain.getEl())){
			throw new FlowSystemException(StrUtil.format("no el content in this unCompile chain[{}]", chain.getChainId()));
		}
		fromChain(chain).compileChain();
	}
```

> Task 4 会在 `compileChain()` 之后追加 `recordCompiledChain` 登记；Task 3 先到此。

- [ ] **Step 4: compileScriptNode 回源钩子**

`FlowBus.java` 的 `compileScriptNode`（约第 273 行）方法开头，在取 `script` 之前插入回源：

```java
	public static void compileScriptNode(Node node) {
		// Rule-DB 模式：脚本影子/失效先回源填 script
		if (com.yomahub.liteflow.repository.RuleDbRuntime.isActive()){
			com.yomahub.liteflow.repository.RuleDbRuntime.ensureScriptLoaded(node.getId());
		}
		String nodeId = node.getId(), name = node.getName(), script = node.getScript(), language = node.getLanguage();
		NodeTypeEnum type = node.getType();
```

（方法其余体不变。）

- [ ] **Step 5: 恢复 BaseRuleDbTest 的 RuleDbRuntime 引用 + 拓宽 cleanup**

取消 Task 2 在 `BaseRuleDbTest.cleanup()` 中注释的 `RuleDbRuntime.destroy();`（恢复 import 与调用），并按 `liteflow-testcase-el-nospring` 的 `BaseTest` 拓宽 cleanup，防止 FlowExecutorHolder 单例跨测试类泄漏：

```java
    @AfterEach
    public void cleanup() {
        com.yomahub.liteflow.repository.RuleDbRuntime.destroy();
        com.yomahub.liteflow.repository.RuleRepositoryHolder.reset();
        com.yomahub.liteflow.core.FlowExecutorHolder.loadInstance().clearStat(); // 若无 clearStat 则用等价清理
        FlowBus.cleanCache();
        FlowBus.clearStat();
        com.yomahub.liteflow.property.LiteflowConfigGetter.clean();
        InMemoryRuleRepository.reset();
    }
```

> 以 nospring `BaseTest`（`@AfterAll` 清理 ComponentScanner/FlowBus/ExecutorHelper/SpiFactoryInitializing/LiteflowConfigGetter/FlowInitHook/LifeCycleHolder）为参照，挑选与 rule-db 测试相关的清理项。若 `FlowExecutorHolder` 无直接清理 API，确保 `FlowBus.cleanCache()` + `FlowBus.clearStat()` + `LiteflowConfigGetter.clean()` 足以让下一个 `buildExecutor` 重建配置生效（executor 是无状态入口，配置经 LiteflowConfigGetter 全局获取）。**目标：多个测试类共存时无状态泄漏。**

- [ ] **Step 6: 写懒加载测试（Task 2 的红测此时应转绿）**

`RuleDbLazyLoadTest.java`：

```java
package com.yomahub.liteflow.test.ruledb;

import com.yomahub.liteflow.core.FlowExecutor;
import com.yomahub.liteflow.flow.LiteflowResponse;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.slot.DefaultContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RuleDbLazyLoadTest extends BaseRuleDbTest {

    @Test
    public void testFirstExecFetchesThenCached() {
        InMemoryRuleRepository.putChain("chain1", "THEN(a, b)");
        registerCommonCmp();
        FlowExecutor executor = buildExecutor(new RuleDbConfig());

        // 启动阶段零回源
        Assertions.assertEquals(0, InMemoryRuleRepository.FETCH_CHAIN_COUNT.get());

        // 首次执行触发一次回源
        LiteflowResponse r1 = executor.execute2Resp("chain1", "arg");
        Assertions.assertTrue(r1.isSuccess());
        Assertions.assertEquals(1, InMemoryRuleRepository.FETCH_CHAIN_COUNT.get());

        // 二次执行零回源（命中缓存）
        LiteflowResponse r2 = executor.execute2Resp("chain1", "arg");
        Assertions.assertTrue(r2.isSuccess());
        Assertions.assertEquals(1, InMemoryRuleRepository.FETCH_CHAIN_COUNT.get());
    }

    @Test
    public void testScriptNodeLazyCompile() {
        InMemoryRuleRepository.putChain("chain2", "THEN(a, s1)");
        InMemoryRuleRepository.putScript("s1", "defaultContext.setData(\"s1\", true);", "script", "groovy");
        registerCommonCmp();
        FlowExecutor executor = buildExecutor(new RuleDbConfig());

        LiteflowResponse r = executor.execute2Resp("chain2", "arg");
        Assertions.assertTrue(r.isSuccess());
        Assertions.assertEquals(1, InMemoryRuleRepository.FETCH_SCRIPT_COUNT.get());
        Assertions.assertEquals(Boolean.TRUE, r.getContextBean(DefaultContext.class).getData("s1"));
    }
}
```

- [ ] **Step 7: 运行测试（Task 2 红测转绿 + Task 3 新测）**

Run: `mvn test -DskipTests=false -pl liteflow-core,liteflow-testcase-el/liteflow-testcase-el-rule-db-core -Dtest=RuleDbStartupTest,RuleDbLazyLoadTest`
Expected: PASS（RuleDbStartupTest 的 1 个方法 + RuleDbLazyLoadTest 的 2 个方法全绿）

若脚本断言失败，对照 `liteflow-testcase-el-script-groovy-springboot` 里脚本的 `defaultContext` 写法对齐。

- [ ] **Step 8: 确认未破坏既有路径（isActive 守卫）**

无需新跑全量。逻辑确认：非 rule-db 应用（classpath 无 RuleRepository 实现）下 `isActive()` 返回 false，三个钩子（FlowExecutor 分流、buildUnCompileChain、compileScriptNode）均为 no-op，既有行为不变。可选冒烟：`mvn clean package -DskipTests -pl liteflow-core` 必须 BUILD SUCCESS。

- [ ] **Step 9: Commit**

```bash
git add liteflow-core liteflow-testcase-el/liteflow-testcase-el-rule-db-core
git commit -m "feat(core): RuleDbRuntime 启动建索引与懒回源接线

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: 有界缓存与淘汰（退影子 + 脚本引用计数）

**Files:**
- Create: `liteflow-core/src/main/java/com/yomahub/liteflow/repository/RuleDbCache.java`
- Modify: `liteflow-core/src/main/java/com/yomahub/liteflow/repository/RuleDbRuntime.java`（init 末尾 init 缓存、destroy 清缓存；编译成功后登记缓存+脚本引用）
- Modify: `liteflow-core/src/main/java/com/yomahub/liteflow/builder/el/LiteFlowChainELBuilder.java`（buildUnCompileChain 的 compileChain 之后登记）
- Test: `liteflow-testcase-el-rule-db-core/.../RuleDbEvictionTest.java`

**Interfaces:**
- Consumes: Task 3 的 RuleDbRuntime；core 既有依赖 Caffeine。
- Produces:
  - `RuleDbCache.init(int capacity)` : void
  - `RuleDbCache.recordChainAccess(String chainId, List<String> scriptNodeIds)` : void —— 编译成功后调用，登记 chain 驻留 + 脚本引用计数 +1
  - `RuleDbCache.scriptRefCount(String nodeId)` : int（测试断言用）
  - `RuleDbCache.destroy()` : void

- [ ] **Step 1: 写 RuleDbCache**

```java
package com.yomahub.liteflow.repository;

import cn.hutool.core.util.ObjectUtil;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.yomahub.liteflow.flow.FlowBus;
import com.yomahub.liteflow.flow.element.Chain;
import com.yomahub.liteflow.flow.element.Node;
import com.yomahub.liteflow.log.LFLog;
import com.yomahub.liteflow.log.LFLoggerManager;
import com.yomahub.liteflow.script.ScriptExecutorFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Rule-DB 有界缓存：容量按 chain 条数；淘汰 chain 时退影子并对其引用脚本引用计数-1，
 * 脚本引用计数归零则 unLoad 并退脚本影子。
 *
 * @author Bryan.Zhang
 * @since 2.16.1
 */
public class RuleDbCache {

    private static final LFLog LOG = LFLoggerManager.getLogger(RuleDbCache.class);

    private static volatile Cache<String, Boolean> chainCache;

    /** chainId -> 它引用的脚本 nodeId 列表 */
    private static final Map<String, List<String>> CHAIN_SCRIPT_REFS = new ConcurrentHashMap<>();

    /** nodeId -> 引用计数 */
    private static final Map<String, AtomicInteger> SCRIPT_REF_COUNT = new ConcurrentHashMap<>();

    public static synchronized void init(int capacity) {
        chainCache = Caffeine.newBuilder()
                .maximumSize(capacity)
                .<String, Boolean>evictionListener((chainId, v, cause) -> onChainEvicted(chainId))
                .build();
        CHAIN_SCRIPT_REFS.clear();
        SCRIPT_REF_COUNT.clear();
    }

    public static void recordChainAccess(String chainId, List<String> scriptNodeIds) {
        if (chainCache == null) {
            return;
        }
        // 更新引用关系（先释放旧引用，再登记新引用，避免重复计数）
        releaseRefs(chainId);
        CHAIN_SCRIPT_REFS.put(chainId, scriptNodeIds);
        for (String nodeId : scriptNodeIds) {
            SCRIPT_REF_COUNT.computeIfAbsent(nodeId, k -> new AtomicInteger(0)).incrementAndGet();
        }
        chainCache.put(chainId, Boolean.TRUE);
    }

    private static void onChainEvicted(String chainId) {
        // 退 chain 影子
        Chain chain = FlowBus.getChain(chainId);
        if (ObjectUtil.isNotNull(chain)) {
            chain.setCompiled(false);
            chain.setConditionList(null);
            chain.setEl(null);
        }
        RuleDbRuntime.onChainEvicted(chainId);
        // 释放脚本引用
        releaseRefs(chainId);
    }

    private static void releaseRefs(String chainId) {
        List<String> refs = CHAIN_SCRIPT_REFS.remove(chainId);
        if (refs == null) {
            return;
        }
        for (String nodeId : refs) {
            AtomicInteger count = SCRIPT_REF_COUNT.get(nodeId);
            if (count != null && count.decrementAndGet() <= 0) {
                SCRIPT_REF_COUNT.remove(nodeId);
                unloadScript(nodeId);
            }
        }
    }

    private static void unloadScript(String nodeId) {
        Node node = FlowBus.getNode(nodeId);
        if (node == null || node.getType() == null || !node.getType().isScript()) {
            return;
        }
        try {
            ScriptExecutorFactory.loadInstance().getScriptExecutor(node.getLanguage()).unLoad(nodeId);
        } catch (Exception e) {
            LOG.warn("unload script[{}] failed: {}", nodeId, e.getMessage());
        }
        node.setScript(null);
        node.setCompiled(false);
        RuleDbRuntime.onScriptEvicted(nodeId);
    }

    public static int scriptRefCount(String nodeId) {
        AtomicInteger c = SCRIPT_REF_COUNT.get(nodeId);
        return c == null ? 0 : c.get();
    }

    public static synchronized void destroy() {
        if (chainCache != null) {
            chainCache.invalidateAll();
            chainCache.cleanUp();
        }
        chainCache = null;
        CHAIN_SCRIPT_REFS.clear();
        SCRIPT_REF_COUNT.clear();
    }
}
```

- [ ] **Step 2: RuleDbRuntime 登记 + 提供 evict 回调 + 收集脚本引用**

在 `RuleDbRuntime` 中：

init() 在 `initialized = true;` 之后、`preload();` 之前加缓存初始化：

```java
        int capacity = 500;
        RuleDbConfig ruleDb = LiteflowConfigGetter.get().getRuleDb();
        if (ruleDb != null && ruleDb.getCacheCapacity() != null) {
            capacity = ruleDb.getCacheCapacity();
        }
        RuleDbCache.init(capacity);
```

新增编译完成后登记方法（用既有 `LiteflowMetaOperator.getNodes(chainId)` 收集脚本节点）：

```java
    /** 编译完成后登记：chain 驻留缓存 + 收集引用的脚本节点（引用计数+1） */
    public static void recordCompiledChain(String chainId) {
        java.util.List<String> scriptRefs = new java.util.ArrayList<>();
        try {
            for (Node n : com.yomahub.liteflow.meta.LiteflowMetaOperator.getNodes(chainId)) {
                if (n.getType() != null && n.getType().isScript() && SCRIPT_VERSION_INDEX.containsKey(n.getId())) {
                    scriptRefs.add(n.getId());
                }
            }
        } catch (Exception ignored) {
        }
        RuleDbCache.recordChainAccess(chainId, scriptRefs);
    }
```

destroy() 开头加 `RuleDbCache.destroy();`（在清索引之前）。

import：`com.yomahub.liteflow.repository.RuleDbCache`、`com.yomahub.liteflow.meta.LiteflowMetaOperator`、`com.yomahub.liteflow.flow.element.Node`（Node 可能已 import）。

- [ ] **Step 3: buildUnCompileChain 编译后登记**

修改 Task 3 Step 3 的 `buildUnCompileChain`，在 `compileChain()` 之后调用登记：

```java
	public static void buildUnCompileChain(Chain chain){
		boolean ruleDbActive = com.yomahub.liteflow.repository.RuleDbRuntime.isActive();
		if (ruleDbActive){
			com.yomahub.liteflow.repository.RuleDbRuntime.ensureChainLoaded(chain.getChainId());
		}
		if (StrUtil.isBlank(chain.getEl())){
			throw new FlowSystemException(StrUtil.format("no el content in this unCompile chain[{}]", chain.getChainId()));
		}
		fromChain(chain).compileChain();
		if (ruleDbActive){
			com.yomahub.liteflow.repository.RuleDbRuntime.recordCompiledChain(chain.getChainId());
		}
	}
```

- [ ] **Step 4: 写淘汰测试**

`RuleDbEvictionTest.java`：

```java
package com.yomahub.liteflow.test.ruledb;

import com.yomahub.liteflow.core.FlowExecutor;
import com.yomahub.liteflow.flow.FlowBus;
import com.yomahub.liteflow.flow.element.Chain;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.repository.RuleDbCache;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RuleDbEvictionTest extends BaseRuleDbTest {

    @Test
    public void testChainEvictedFallsBackToShadowAndReloadable() {
        for (int i = 1; i <= 6; i++) {
            InMemoryRuleRepository.putChain("chain" + i, "THEN(a, b)");
        }
        registerCommonCmp();
        RuleDbConfig cfg = new RuleDbConfig();
        cfg.setCacheCapacity(2); // 小容量强制淘汰
        FlowExecutor executor = buildExecutor(cfg);

        for (int i = 1; i <= 6; i++) {
            Assertions.assertTrue(executor.execute2Resp("chain" + i, "arg").isSuccess());
        }
        // 触发 Caffeine 清理
        try { Thread.sleep(100); } catch (InterruptedException ignored) {}

        // 至少有 chain 退回影子（未编译）
        int shadow = 0;
        for (Chain c : FlowBus.getChainMap().values()) {
            if (!c.isCompiled()) {
                shadow++;
            }
        }
        Assertions.assertTrue(shadow > 0, "expected some chains evicted to shadow state");

        // 被淘汰的 chain 仍可重新执行（重新回源编译）
        Assertions.assertTrue(executor.execute2Resp("chain1", "arg").isSuccess());
    }

    @Test
    public void testScriptRefCountUnloadOnEviction() {
        InMemoryRuleRepository.putChain("cs1", "THEN(a, sx)");
        InMemoryRuleRepository.putChain("cs2", "THEN(b, sx)");
        InMemoryRuleRepository.putScript("sx", "defaultContext.setData(\"sx\", true);", "script", "groovy");
        registerCommonCmp();
        RuleDbConfig cfg = new RuleDbConfig();
        cfg.setCacheCapacity(1); // 一次只容一个 chain
        FlowExecutor executor = buildExecutor(cfg);

        executor.execute2Resp("cs1", "arg");
        executor.execute2Resp("cs2", "arg");
        try { Thread.sleep(100); } catch (InterruptedException ignored) {}

        // 两个 chain 引用同一脚本，容量 1 时最终至多一个 chain 驻留，引用计数 <= 1
        Assertions.assertTrue(RuleDbCache.scriptRefCount("sx") <= 1);
    }
}
```

- [ ] **Step 5: 运行测试**

Run: `mvn test -DskipTests=false -pl liteflow-core,liteflow-testcase-el/liteflow-testcase-el-rule-db-core -Dtest=RuleDbEvictionTest`
Expected: PASS

- [ ] **Step 6: 回归 Task 2/3 测试**

Run: `mvn test -DskipTests=false -pl liteflow-testcase-el/liteflow-testcase-el-rule-db-core`
Expected: 全绿（Startup + LazyLoad + Eviction）

- [ ] **Step 7: Commit**

```bash
git add liteflow-core liteflow-testcase-el/liteflow-testcase-el-rule-db-core
git commit -m "feat(core): Rule-DB 有界缓存与淘汰（退影子+脚本引用计数）

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: 变更同步与对账（ChangeWatcher + Reconciler + 分级刷新）

**Files:**
- Create: `liteflow-core/src/main/java/com/yomahub/liteflow/repository/RuleDbSyncManager.java`
- Modify: `liteflow-core/src/main/java/com/yomahub/liteflow/repository/RuleDbRuntime.java`（init 末尾启动同步、destroy 停止；暴露 applyChange/reconcile 供 SyncManager 回调）
- Test: `liteflow-testcase-el-rule-db-core/.../RuleDbConvergeTest.java`

**Interfaces:**
- Consumes: Task 3 的索引/回源、Task 4 的缓存。
- Produces:
  - `RuleDbSyncManager.start()` : void —— 启动 seq 轮询线程 + 订阅 + 对账线程
  - `RuleDbSyncManager.stop()` : void
  - `RuleDbSyncManager.pollOnce()` : void（测试可直接调，绕过定时）
  - `RuleDbSyncManager.reconcileOnce()` : void（测试可直接调）
  - `RuleDbRuntime.applyChange(ChangeRecord)` : void —— 单条变更落到索引/缓存（分级刷新）
  - `RuleDbRuntime.reconcile(RuleManifest)` : void —— 全量对账

- [ ] **Step 1: 写 RuleDbRuntime.applyChange 与 reconcile（分级刷新逻辑）**

在 `RuleDbRuntime` 追加：

```java
    /**
     * 单条变更处理（分级刷新）：
     * - UPSERT chain：更新索引版本；若该 chain 当前驻留缓存，失效缓存态使下次执行懒加载新版；影子则只更新索引。
     * - UPSERT script：更新索引；若脚本当前驻留，清缓存态让下次 getInstance 回源重编。
     * - DELETE：移除索引 + FlowBus 中的条目。
     */
    public static void applyChange(com.yomahub.liteflow.repository.vo.ChangeRecord change) {
        String id = change.getTargetId();
        long version = change.getVersion();
        if (change.getTargetType() == com.yomahub.liteflow.repository.vo.ChangeRecord.TargetType.CHAIN) {
            if (change.getOp() == com.yomahub.liteflow.repository.vo.ChangeRecord.Op.DELETE) {
                CHAIN_VERSION_INDEX.remove(id);
                CHAIN_CACHED_VERSION.remove(id);
                FlowBus.removeChain(id);
            } else {
                Long cur = CHAIN_VERSION_INDEX.get(id);
                CHAIN_VERSION_INDEX.put(id, version);
                if (cur == null) {
                    // 新增 chain：注册影子
                    FlowBus.addChain(id);
                }
                // 缓存态失效：置为过期版本，下次 ensureChainLoaded 会回源
                CHAIN_CACHED_VERSION.remove(id);
                Chain chain = FlowBus.getChain(id);
                if (chain != null) {
                    chain.setCompiled(false);
                    chain.setConditionList(null);
                    chain.setEl(null);
                }
            }
        } else {
            if (change.getOp() == com.yomahub.liteflow.repository.vo.ChangeRecord.Op.DELETE) {
                SCRIPT_VERSION_INDEX.remove(id);
                SCRIPT_CACHED_VERSION.remove(id);
                FlowBus.unloadScriptNode(id);
            } else {
                SCRIPT_VERSION_INDEX.put(id, version);
                SCRIPT_CACHED_VERSION.remove(id);
                Node node = FlowBus.getNode(id);
                if (node != null) {
                    node.setScript(null);
                    node.setCompiled(false);
                }
            }
        }
    }

    /** 全量对账：以 manifest 为准修正索引；version 相同再比 md5 兜底（此处 version 单调即可，md5 交由 SQL/Redis 实现在 fetch 时保证） */
    public static void reconcile(com.yomahub.liteflow.repository.vo.RuleManifest manifest) {
        java.util.Set<String> liveChains = new java.util.HashSet<>();
        if (manifest.getChains() != null) {
            for (ChainMeta cm : manifest.getChains()) {
                liveChains.add(cm.getChainId());
                Long cur = CHAIN_VERSION_INDEX.get(cm.getChainId());
                if (cur == null || cur != cm.getVersion()) {
                    applyChange(new com.yomahub.liteflow.repository.vo.ChangeRecord(
                            0, com.yomahub.liteflow.repository.vo.ChangeRecord.TargetType.CHAIN,
                            cm.getChainId(), com.yomahub.liteflow.repository.vo.ChangeRecord.Op.UPSERT, cm.getVersion()));
                }
            }
        }
        // 清单中已消失的 chain（且属 rule-db 管理）删除
        for (String chainId : new java.util.ArrayList<>(CHAIN_VERSION_INDEX.keySet())) {
            if (!liveChains.contains(chainId)) {
                applyChange(new com.yomahub.liteflow.repository.vo.ChangeRecord(
                        0, com.yomahub.liteflow.repository.vo.ChangeRecord.TargetType.CHAIN,
                        chainId, com.yomahub.liteflow.repository.vo.ChangeRecord.Op.DELETE, 0));
            }
        }
        java.util.Set<String> liveScripts = new java.util.HashSet<>();
        if (manifest.getScripts() != null) {
            for (ScriptMeta sm : manifest.getScripts()) {
                liveScripts.add(sm.getNodeId());
                Long cur = SCRIPT_VERSION_INDEX.get(sm.getNodeId());
                if (cur == null) {
                    registerShadowScriptMeta(sm);
                } else if (cur != sm.getVersion()) {
                    applyChange(new com.yomahub.liteflow.repository.vo.ChangeRecord(
                            0, com.yomahub.liteflow.repository.vo.ChangeRecord.TargetType.SCRIPT,
                            sm.getNodeId(), com.yomahub.liteflow.repository.vo.ChangeRecord.Op.UPSERT, sm.getVersion()));
                }
            }
        }
        for (String nodeId : new java.util.ArrayList<>(SCRIPT_VERSION_INDEX.keySet())) {
            if (!liveScripts.contains(nodeId)) {
                applyChange(new com.yomahub.liteflow.repository.vo.ChangeRecord(
                        0, com.yomahub.liteflow.repository.vo.ChangeRecord.TargetType.SCRIPT,
                        nodeId, com.yomahub.liteflow.repository.vo.ChangeRecord.Op.DELETE, 0));
            }
        }
    }
```

> 分级刷新说明：设计文档要求"驻留条目后台异步刷新、旧版继续服务"。本实现采用等价的**惰性失效**（失效缓存态 → 下次执行懒加载新版），配合执行中持有旧 conditionList 引用跑完的既有语义（Chain.execute:132 注释），达到"变更后旧执行不中断、新执行拿新版、秒级窗口收敛"的效果，且实现更简单、无额外刷新线程与刷新失败态。若后续需要"零首个请求延迟"，可将驻留条目改为后台预编译——留作增强，不在本计划范围。对应更新设计文档 §8.4 的实现注记（Task 5 Step 5 附带提交）。

- [ ] **Step 2: 写 RuleDbSyncManager**

```java
package com.yomahub.liteflow.repository;

import com.yomahub.liteflow.exception.SeqGapException;
import com.yomahub.liteflow.log.LFLog;
import com.yomahub.liteflow.log.LFLoggerManager;
import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import com.yomahub.liteflow.repository.vo.RuleManifest;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * 变更同步：seq 轮询（兜底/主感知）+ 可选订阅推送 + 周期对账。
 *
 * @author Bryan.Zhang
 * @since 2.16.1
 */
public class RuleDbSyncManager {

    private static final LFLog LOG = LFLoggerManager.getLogger(RuleDbSyncManager.class);

    private static volatile ScheduledExecutorService scheduler;

    public static synchronized void start() {
        RuleDbConfig cfg = LiteflowConfigGetter.get().getRuleDb();
        int seqPoll = seqPollSeconds(cfg);
        int reconcile = cfg == null || cfg.getReconcileSeconds() == null ? 60 : cfg.getReconcileSeconds();

        scheduler = Executors.newScheduledThreadPool(2, daemonFactory());
        scheduler.scheduleWithFixedDelay(RuleDbSyncManager::pollOnceSafe, seqPoll, seqPoll, TimeUnit.SECONDS);
        scheduler.scheduleWithFixedDelay(RuleDbSyncManager::reconcileOnceSafe, reconcile, reconcile, TimeUnit.SECONDS);

        // 可选订阅推送（Redis 实现；SQL 空实现）
        try {
            RuleRepositoryHolder.get().subscribe(changes -> {
                for (ChangeRecord c : changes) {
                    RuleDbRuntime.applyChange(c);
                    advanceSeq(c.getSeq());
                }
            });
        } catch (Exception e) {
            LOG.warn("rule-db subscribe failed, fallback to polling: {}", e.getMessage());
        }
    }

    private static int seqPollSeconds(RuleDbConfig cfg) {
        if (cfg != null && cfg.getSeqPollSeconds() != null) {
            return cfg.getSeqPollSeconds();
        }
        return 3; // 未配置时的通用默认；插件可通过配置覆盖（Redis 建议 30）
    }

    public static void pollOnce() {
        RuleRepository repo = RuleRepositoryHolder.get();
        long last = RuleDbRuntime.LAST_APPLIED_SEQ.get();
        long latest = repo.fetchLatestSeq();
        if (latest <= last) {
            return;
        }
        try {
            List<ChangeRecord> changes = repo.fetchChangesSince(last);
            for (ChangeRecord c : changes) {
                RuleDbRuntime.applyChange(c);
                advanceSeq(c.getSeq());
            }
        } catch (SeqGapException gap) {
            LOG.warn("seq gap detected, trigger full reconcile: {}", gap.getMessage());
            reconcileOnce();
        }
    }

    public static void reconcileOnce() {
        RuleManifest manifest = RuleRepositoryHolder.get().fetchManifest();
        RuleDbRuntime.reconcile(manifest);
        advanceSeq(manifest.getLatestSeq());
    }

    private static void advanceSeq(long seq) {
        long cur;
        do {
            cur = RuleDbRuntime.LAST_APPLIED_SEQ.get();
            if (seq <= cur) {
                return;
            }
        } while (!RuleDbRuntime.LAST_APPLIED_SEQ.compareAndSet(cur, seq));
    }

    private static void pollOnceSafe() {
        try {
            pollOnce();
        } catch (Exception e) {
            LOG.warn("rule-db poll failed: {}", e.getMessage());
        }
    }

    private static void reconcileOnceSafe() {
        try {
            reconcileOnce();
        } catch (Exception e) {
            LOG.warn("rule-db reconcile failed: {}", e.getMessage());
        }
    }

    private static ThreadFactory daemonFactory() {
        return r -> {
            Thread t = new Thread(r, "liteflow-rule-db-sync");
            t.setDaemon(true);
            return t;
        };
    }

    public static synchronized void stop() {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
    }
}
```

- [ ] **Step 3: RuleDbRuntime.init 末尾 start，destroy 里 stop**

init() 在 `preload();` 之前加 `RuleDbSyncManager.start();`（先启动同步再预热无妨；预热失败也不影响）。实际顺序：`RuleDbCache.init(capacity); RuleDbSyncManager.start(); preload();`。
destroy() 开头加 `RuleDbSyncManager.stop();`。

- [ ] **Step 4: 写收敛测试（用 pollOnce / reconcileOnce 直驱，避免等定时）**

`RuleDbConvergeTest.java`：

```java
package com.yomahub.liteflow.test.ruledb;

import com.yomahub.liteflow.core.FlowExecutor;
import com.yomahub.liteflow.flow.LiteflowResponse;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.repository.RuleDbSyncManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RuleDbConvergeTest extends BaseRuleDbTest {

    @Test
    public void testChangeConvergesViaPoll() {
        InMemoryRuleRepository.publishChain("chain1", "THEN(a, b)");
        registerCommonCmp();
        FlowExecutor executor = buildExecutor(new RuleDbConfig());

        LiteflowResponse r1 = executor.execute2Resp("chain1", "arg");
        Assertions.assertEquals("a==>b", r1.getExecuteStepStr());

        // 另一节点发布新版
        InMemoryRuleRepository.publishChain("chain1", "THEN(b, a)");
        // 感知变更（等价于轮询周期到达）
        RuleDbSyncManager.pollOnce();

        LiteflowResponse r2 = executor.execute2Resp("chain1", "arg");
        Assertions.assertEquals("b==>a", r2.getExecuteStepStr());
    }

    @Test
    public void testLostNotificationConvergesViaReconcile() {
        InMemoryRuleRepository.publishChain("chain1", "THEN(a, b)");
        registerCommonCmp();
        FlowExecutor executor = buildExecutor(new RuleDbConfig());
        executor.execute2Resp("chain1", "arg");

        // 绕过发布规范直接改内容（丢通知）：putChain 不记 change_log
        InMemoryRuleRepository.putChain("chain1", "THEN(b, a)");
        // 轮询感知不到（seq 未变），对账兜底才收敛
        RuleDbSyncManager.pollOnce();
        LiteflowResponse rPoll = executor.execute2Resp("chain1", "arg");
        Assertions.assertEquals("a==>b", rPoll.getExecuteStepStr()); // 仍旧版

        RuleDbSyncManager.reconcileOnce();
        LiteflowResponse rReconcile = executor.execute2Resp("chain1", "arg");
        Assertions.assertEquals("b==>a", rReconcile.getExecuteStepStr()); // 对账后新版
    }

    @Test
    public void testSeqGapTriggersReconcile() {
        InMemoryRuleRepository.publishChain("chain1", "THEN(a, b)");
        registerCommonCmp();
        FlowExecutor executor = buildExecutor(new RuleDbConfig());
        executor.execute2Resp("chain1", "arg");

        InMemoryRuleRepository.publishChain("chain1", "THEN(b, a)");
        // 模拟 change_log 被清理：MIN_SEQ 抬高到当前 seq 之上，制造断档
        InMemoryRuleRepository.MIN_SEQ = InMemoryRuleRepository.SEQ.get() + 1;

        RuleDbSyncManager.pollOnce(); // 内部捕获 SeqGapException 转对账
        LiteflowResponse r = executor.execute2Resp("chain1", "arg");
        Assertions.assertEquals("b==>a", r.getExecuteStepStr());
    }
}
```

- [ ] **Step 5: 运行测试并更新设计文档实现注记**

Run: `mvn test -DskipTests=false -pl liteflow-core,liteflow-testcase-el/liteflow-testcase-el-rule-db-core -Dtest=RuleDbConvergeTest`
Expected: PASS（3 方法）

在 `docs/superpowers/specs/2026-07-10-rule-db-plugin-design.md` §8.4 末尾加一行实现注记：

```markdown
> 实现注记（2026-07-10）：v1 的"分级刷新"以惰性失效落地——驻留条目收到变更后失效缓存态、下次执行懒加载新版，配合执行中持有旧 conditionList 引用跑完的既有语义达成不中断切换。"后台预编译零首个请求延迟"作为后续增强。
```

- [ ] **Step 6: Commit**

```bash
git add liteflow-core liteflow-testcase-el/liteflow-testcase-el-rule-db-core docs/superpowers/specs/2026-07-10-rule-db-plugin-design.md
git commit -m "feat(core): Rule-DB 变更同步与对账（轮询+订阅+对账兜底）

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: 降级与容错测试（无新生产代码，纯行为固化）

**Files:**
- Test: `liteflow-testcase-el-rule-db-core/.../RuleDbDegradeTest.java`

**Interfaces:**
- Consumes: 前序全部；断言 `ChainLoadException` 语义与存储不可用时缓存命中仍可执行。

- [ ] **Step 1: 写降级测试**

```java
package com.yomahub.liteflow.test.ruledb;

import com.yomahub.liteflow.core.FlowExecutor;
import com.yomahub.liteflow.flow.LiteflowResponse;
import com.yomahub.liteflow.property.RuleDbConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RuleDbDegradeTest extends BaseRuleDbTest {

    @Test
    public void testCachedChainStillExecutesWhenStorageDown() {
        InMemoryRuleRepository.putChain("chain1", "THEN(a, b)");
        registerCommonCmp();
        FlowExecutor executor = buildExecutor(new RuleDbConfig());
        // 先执行一次，进缓存
        Assertions.assertTrue(executor.execute2Resp("chain1", "arg").isSuccess());

        // 存储宕机
        InMemoryRuleRepository.DOWN = true;
        // 缓存命中仍可执行（零回源）
        Assertions.assertTrue(executor.execute2Resp("chain1", "arg").isSuccess());
    }

    @Test
    public void testUncachedChainFailsWithChainLoadExceptionWhenStorageDown() {
        InMemoryRuleRepository.putChain("chain1", "THEN(a, b)");
        InMemoryRuleRepository.putChain("chain2", "THEN(b, a)");
        registerCommonCmp();
        RuleDbConfig cfg = new RuleDbConfig();
        cfg.setFetchRetryTimes(1);
        FlowExecutor executor = buildExecutor(cfg);
        executor.execute2Resp("chain1", "arg"); // 只缓存 chain1

        InMemoryRuleRepository.DOWN = true;
        // chain2 未缓存，回源失败 → response 携带 ChainLoadException
        LiteflowResponse r = executor.execute2Resp("chain2", "arg");
        Assertions.assertFalse(r.isSuccess());
        Assertions.assertTrue(r.getCause() instanceof com.yomahub.liteflow.exception.ChainLoadException
                || r.getCause().getCause() instanceof com.yomahub.liteflow.exception.ChainLoadException);
    }
}
```

- [ ] **Step 2: 运行测试**

Run: `mvn test -DskipTests=false -pl liteflow-core,liteflow-testcase-el/liteflow-testcase-el-rule-db-core -Dtest=RuleDbDegradeTest`
Expected: PASS

若 `getCause` 层级与断言不符，先打印 `r.getCause()` 的实际类型链再调整断言（不要改生产代码来迁就断言）。

- [ ] **Step 3: 全量回归 core 测试模块**

Run: `mvn test -DskipTests=false -pl liteflow-testcase-el/liteflow-testcase-el-rule-db-core`
Expected: 全绿

- [ ] **Step 4: Commit**

```bash
git add liteflow-testcase-el/liteflow-testcase-el-rule-db-core
git commit -m "test(rule-db): 降级与容错行为固化

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 7: liteflow-rule-db-sql 插件（SqlRuleRepository + JDBC 连接 + DDL）

**Files:**
- Create: `liteflow-rule-plugin/liteflow-rule-db-sql/pom.xml`
- Modify: `liteflow-rule-plugin/pom.xml`（modules 加 `liteflow-rule-db-sql`）
- Create: `liteflow-rule-plugin/liteflow-rule-db-sql/src/main/java/com/yomahub/liteflow/repository/sql/SqlRuleRepository.java`
- Create: `.../sql/SqlConnectionManager.java`（连接获取：优先容器 DataSource，其次 url 直连）
- Create: `.../sql/SqlDialect.java`（表名拼装 + DDL 文本 + 建表）
- Create: `liteflow-rule-plugin/liteflow-rule-db-sql/src/main/resources/META-INF/services/com.yomahub.liteflow.repository.RuleRepository`
- Create: `liteflow-rule-plugin/liteflow-rule-db-sql/src/main/resources/sql/ddl-mysql.sql` 与 `ddl-h2.sql`

**Interfaces:**
- Consumes: core 的 `RuleRepository` 及 VO、`LiteflowConfigGetter.get().getRuleDb()`。
- Produces: `SqlRuleRepository`（无参构造，ServiceLoader 装载）；`SqlConnectionManager.getConnection()`；`SqlDialect.chainTable()/scriptTable()/changeLogTable()/createTablesIfAbsent(conn)`。

- [ ] **Step 1: 建插件 pom 并挂到聚合 pom**

`liteflow-rule-plugin/liteflow-rule-db-sql/pom.xml`：

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <parent>
        <artifactId>liteflow-rule-plugin</artifactId>
        <groupId>com.yomahub</groupId>
        <version>${revision}</version>
        <relativePath>../pom.xml</relativePath>
    </parent>
    <modelVersion>4.0.0</modelVersion>
    <artifactId>liteflow-rule-db-sql</artifactId>
    <name>${project.artifactId}</name>
    <dependencies>
        <dependency>
            <groupId>com.yomahub</groupId>
            <artifactId>liteflow-core</artifactId>
            <version>${revision}</version>
        </dependency>
    </dependencies>
</project>
```

`liteflow-rule-plugin/pom.xml` 的 `<modules>` 里在 `liteflow-rule-redis` 后加 `<module>liteflow-rule-db-sql</module>`。

- [ ] **Step 2: 写 SqlConnectionManager（容器 DataSource 优先，url 直连兜底）**

```java
package com.yomahub.liteflow.repository.sql;

import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.spi.holder.ContextAwareHolder;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * SQL 连接获取：优先复用容器 DataSource（datasource-bean-name 指定或自动查找），
 * 否则用 url/username/password 直连。
 *
 * @author Bryan.Zhang
 * @since 2.16.1
 */
public class SqlConnectionManager {

    private volatile DataSource dataSource;

    private volatile boolean resolved = false;

    public Connection getConnection() throws SQLException {
        RuleDbConfig cfg = LiteflowConfigGetter.get().getRuleDb();
        DataSource ds = resolveDataSource(cfg);
        if (ds != null) {
            return ds.getConnection();
        }
        if (StrUtil.isBlank(cfg.getUrl())) {
            throw new ConfigErrorException("rule-db sql: neither a DataSource bean nor liteflow.rule-db.url is available");
        }
        loadDriverIfNeeded(cfg);
        return DriverManager.getConnection(cfg.getUrl(), cfg.getUsername(), cfg.getPassword());
    }

    private DataSource resolveDataSource(RuleDbConfig cfg) {
        if (resolved) {
            return dataSource;
        }
        synchronized (this) {
            if (resolved) {
                return dataSource;
            }
            // url 显式配置时优先直连，不夺容器 DataSource
            if (StrUtil.isBlank(cfg.getUrl())) {
                dataSource = lookupDataSourceBean(cfg.getDatasourceBeanName());
            }
            resolved = true;
            return dataSource;
        }
    }

    private DataSource lookupDataSourceBean(String beanName) {
        try {
            if (StrUtil.isNotBlank(beanName)) {
                return ContextAwareHolder.loadContextAware().getBean(beanName);
            }
            return ContextAwareHolder.loadContextAware().getBean(DataSource.class);
        } catch (Exception e) {
            return null;
        }
    }

    private void loadDriverIfNeeded(RuleDbConfig cfg) {
        String driver = cfg.getDriverClassName();
        if (StrUtil.isBlank(driver)) {
            driver = guessDriver(cfg.getUrl());
        }
        if (StrUtil.isNotBlank(driver)) {
            try {
                Class.forName(driver);
            } catch (ClassNotFoundException e) {
                throw new ConfigErrorException("rule-db sql: driver class not found: " + driver);
            }
        }
    }

    private String guessDriver(String url) {
        if (url == null) {
            return null;
        }
        if (url.startsWith("jdbc:mysql")) {
            return "com.mysql.cj.jdbc.Driver";
        }
        if (url.startsWith("jdbc:h2")) {
            return "org.h2.Driver";
        }
        if (url.startsWith("jdbc:postgresql")) {
            return "org.postgresql.Driver";
        }
        return null; // 交给 DriverManager 的 SPI 自发现
    }
}
```

注意：`ContextAwareHolder.loadContextAware().getBean(Class)` 的签名以 core 中 `ContextAware` 接口为准（打开确认是否有按类型取 bean 的方法；若只有按名，则改为按常见 bean 名 "dataSource" 查找，并在文档要求多数据源时显式配 `datasource-bean-name`）。

- [ ] **Step 3: 写 SqlDialect（表名 + DDL + 建表）**

```java
package com.yomahub.liteflow.repository.sql;

import cn.hutool.core.io.resource.ResourceUtil;
import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbConfig;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 表名拼装（前缀可配，字段名固定）+ 建表 DDL。
 *
 * @author Bryan.Zhang
 * @since 2.16.1
 */
public class SqlDialect {

    public String prefix() {
        RuleDbConfig cfg = LiteflowConfigGetter.get().getRuleDb();
        String p = cfg == null ? null : cfg.getTablePrefix();
        return StrUtil.isBlank(p) ? "lf_" : p;
    }

    public String chainTable() {
        return prefix() + "chain";
    }

    public String scriptTable() {
        return prefix() + "script";
    }

    public String changeLogTable() {
        return prefix() + "change_log";
    }

    /** DDL 文本（缺表报错时给用户复制执行） */
    public String ddlText() {
        String tpl = ResourceUtil.readUtf8Str("sql/ddl-mysql.sql");
        return applyPrefix(tpl);
    }

    private String applyPrefix(String tpl) {
        return tpl.replace("${prefix}", prefix());
    }

    /** auto-init-table=true 时建表（用 h2/通用 DDL） */
    public void createTablesIfAbsent(Connection conn) throws SQLException {
        String ddl = applyPrefix(ResourceUtil.readUtf8Str("sql/ddl-h2.sql"));
        try (Statement st = conn.createStatement()) {
            for (String stmt : ddl.split(";")) {
                if (StrUtil.isNotBlank(stmt)) {
                    st.execute(stmt);
                }
            }
        }
    }
}
```

`src/main/resources/sql/ddl-mysql.sql`（`${prefix}` 占位，字段严格对齐设计 §6.1）：

```sql
CREATE TABLE IF NOT EXISTS `${prefix}chain` (
  `application_name` VARCHAR(64) NOT NULL,
  `chain_id` VARCHAR(128) NOT NULL,
  `namespace` VARCHAR(64) DEFAULT NULL,
  `el_data` TEXT NOT NULL,
  `route_data` TEXT DEFAULT NULL,
  `version` BIGINT NOT NULL DEFAULT 1,
  `content_md5` CHAR(32) NOT NULL,
  `enable` TINYINT NOT NULL DEFAULT 1,
  `gmt_create` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `gmt_modified` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`application_name`, `chain_id`)
);
CREATE TABLE IF NOT EXISTS `${prefix}script` (
  `application_name` VARCHAR(64) NOT NULL,
  `node_id` VARCHAR(128) NOT NULL,
  `script_name` VARCHAR(128) DEFAULT NULL,
  `script_type` VARCHAR(32) NOT NULL,
  `script_language` VARCHAR(32) DEFAULT NULL,
  `script_data` TEXT NOT NULL,
  `version` BIGINT NOT NULL DEFAULT 1,
  `content_md5` CHAR(32) NOT NULL,
  `enable` TINYINT NOT NULL DEFAULT 1,
  `gmt_create` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `gmt_modified` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`application_name`, `node_id`)
);
CREATE TABLE IF NOT EXISTS `${prefix}change_log` (
  `seq` BIGINT NOT NULL AUTO_INCREMENT,
  `application_name` VARCHAR(64) NOT NULL,
  `target_type` VARCHAR(16) NOT NULL,
  `target_id` VARCHAR(128) NOT NULL,
  `op` VARCHAR(16) NOT NULL,
  `version` BIGINT NOT NULL,
  `gmt_create` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`seq`),
  KEY `idx_app_seq` (`application_name`, `seq`)
);
```

`src/main/resources/sql/ddl-h2.sql`（H2 兼容：去反引号、AUTO_INCREMENT→auto_increment、KEY 拆成独立 INDEX）：

```sql
CREATE TABLE IF NOT EXISTS ${prefix}chain (
  application_name VARCHAR(64) NOT NULL,
  chain_id VARCHAR(128) NOT NULL,
  namespace VARCHAR(64) DEFAULT NULL,
  el_data CLOB NOT NULL,
  route_data CLOB DEFAULT NULL,
  version BIGINT NOT NULL DEFAULT 1,
  content_md5 CHAR(32) NOT NULL,
  enable TINYINT NOT NULL DEFAULT 1,
  gmt_create TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  gmt_modified TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (application_name, chain_id)
);
CREATE TABLE IF NOT EXISTS ${prefix}script (
  application_name VARCHAR(64) NOT NULL,
  node_id VARCHAR(128) NOT NULL,
  script_name VARCHAR(128) DEFAULT NULL,
  script_type VARCHAR(32) NOT NULL,
  script_language VARCHAR(32) DEFAULT NULL,
  script_data CLOB NOT NULL,
  version BIGINT NOT NULL DEFAULT 1,
  content_md5 CHAR(32) NOT NULL,
  enable TINYINT NOT NULL DEFAULT 1,
  gmt_create TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  gmt_modified TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (application_name, node_id)
);
CREATE TABLE IF NOT EXISTS ${prefix}change_log (
  seq BIGINT NOT NULL AUTO_INCREMENT,
  application_name VARCHAR(64) NOT NULL,
  target_type VARCHAR(16) NOT NULL,
  target_id VARCHAR(128) NOT NULL,
  op VARCHAR(16) NOT NULL,
  version BIGINT NOT NULL,
  gmt_create TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (seq)
);
CREATE INDEX IF NOT EXISTS idx_app_seq ON ${prefix}change_log (application_name, seq);
```

- [ ] **Step 4: 写 SqlRuleRepository**

```java
package com.yomahub.liteflow.repository.sql;

import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.exception.SeqGapException;
import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.repository.RuleRepository;
import com.yomahub.liteflow.repository.vo.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Rule-DB 的 SQL 权威源实现。
 *
 * @author Bryan.Zhang
 * @since 2.16.1
 */
public class SqlRuleRepository implements RuleRepository {

    private final SqlConnectionManager connectionManager = new SqlConnectionManager();

    private final SqlDialect dialect = new SqlDialect();

    private volatile boolean tableChecked = false;

    private String app() {
        RuleDbConfig cfg = LiteflowConfigGetter.get().getRuleDb();
        String name = cfg == null ? null : cfg.getApplicationName();
        return StrUtil.isBlank(name) ? "default" : name;
    }

    private Connection conn() throws SQLException {
        Connection c = connectionManager.getConnection();
        ensureTables(c);
        return c;
    }

    private synchronized void ensureTables(Connection c) {
        if (tableChecked) {
            return;
        }
        RuleDbConfig cfg = LiteflowConfigGetter.get().getRuleDb();
        if (cfg != null && Boolean.TRUE.equals(cfg.getAutoInitTable())) {
            try {
                dialect.createTablesIfAbsent(c);
            } catch (SQLException e) {
                throw new RuntimeException("auto init rule-db tables failed: " + e.getMessage(), e);
            }
        }
        tableChecked = true;
    }

    @Override
    public RuleManifest fetchManifest() {
        RuleManifest manifest = new RuleManifest();
        List<ChainMeta> chains = new ArrayList<>();
        List<ScriptMeta> scripts = new ArrayList<>();
        String chainSql = "SELECT chain_id, version, content_md5 FROM " + dialect.chainTable()
                + " WHERE application_name = ? AND enable = 1";
        String scriptSql = "SELECT node_id, version, content_md5, script_type, script_language, script_name FROM "
                + dialect.scriptTable() + " WHERE application_name = ? AND enable = 1";
        try (Connection c = conn()) {
            try (PreparedStatement ps = c.prepareStatement(chainSql)) {
                ps.setString(1, app());
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        chains.add(new ChainMeta(rs.getString(1), rs.getLong(2), rs.getString(3)));
                    }
                }
            }
            try (PreparedStatement ps = c.prepareStatement(scriptSql)) {
                ps.setString(1, app());
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        scripts.add(new ScriptMeta(rs.getString(1), rs.getLong(2), rs.getString(3),
                                rs.getString(4), rs.getString(5), rs.getString(6)));
                    }
                }
            }
            manifest.setChains(chains);
            manifest.setScripts(scripts);
            manifest.setLatestSeq(fetchLatestSeq());
        } catch (SQLException e) {
            throw wrap("fetchManifest", e);
        }
        return manifest;
    }

    @Override
    public ChainRecord fetchChain(String chainId) {
        String sql = "SELECT chain_id, el_data, route_data, namespace, version, content_md5, enable FROM "
                + dialect.chainTable() + " WHERE application_name = ? AND chain_id = ?";
        try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, app());
            ps.setString(2, chainId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                ChainRecord r = new ChainRecord();
                r.setChainId(rs.getString(1));
                r.setEl(rs.getString(2));
                r.setRoute(rs.getString(3));
                r.setNamespace(rs.getString(4));
                r.setVersion(rs.getLong(5));
                r.setMd5(rs.getString(6));
                r.setEnable(rs.getInt(7) == 1);
                return r;
            }
        } catch (SQLException e) {
            throw wrap("fetchChain", e);
        }
    }

    @Override
    public ScriptRecord fetchScript(String nodeId) {
        String sql = "SELECT node_id, script_data, script_name, script_type, script_language, version, content_md5, enable FROM "
                + dialect.scriptTable() + " WHERE application_name = ? AND node_id = ?";
        try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, app());
            ps.setString(2, nodeId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                ScriptRecord r = new ScriptRecord();
                r.setNodeId(rs.getString(1));
                r.setScript(rs.getString(2));
                r.setName(rs.getString(3));
                r.setType(rs.getString(4));
                r.setLanguage(rs.getString(5));
                r.setVersion(rs.getLong(6));
                r.setMd5(rs.getString(7));
                r.setEnable(rs.getInt(8) == 1);
                return r;
            }
        } catch (SQLException e) {
            throw wrap("fetchScript", e);
        }
    }

    @Override
    public long fetchLatestSeq() {
        String sql = "SELECT MAX(seq) FROM " + dialect.changeLogTable() + " WHERE application_name = ?";
        try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, app());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    long v = rs.getLong(1);
                    return rs.wasNull() ? 0 : v;
                }
                return 0;
            }
        } catch (SQLException e) {
            throw wrap("fetchLatestSeq", e);
        }
    }

    @Override
    public List<ChangeRecord> fetchChangesSince(long seq) {
        // 断档检测：若存在记录但最小 seq 已大于 seq+1，说明中间被清理
        String minSql = "SELECT MIN(seq) FROM " + dialect.changeLogTable() + " WHERE application_name = ?";
        String listSql = "SELECT seq, target_type, target_id, op, version FROM " + dialect.changeLogTable()
                + " WHERE application_name = ? AND seq > ? ORDER BY seq ASC";
        try (Connection c = conn()) {
            try (PreparedStatement ps = c.prepareStatement(minSql)) {
                ps.setString(1, app());
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        long min = rs.getLong(1);
                        if (!rs.wasNull() && seq > 0 && min > seq + 1) {
                            throw new SeqGapException("change log gap: since=" + seq + " min=" + min);
                        }
                    }
                }
            }
            List<ChangeRecord> result = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement(listSql)) {
                ps.setString(1, app());
                ps.setLong(2, seq);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        result.add(new ChangeRecord(rs.getLong(1),
                                ChangeRecord.TargetType.valueOf(rs.getString(2)),
                                rs.getString(3), ChangeRecord.Op.valueOf(rs.getString(4)), rs.getLong(5)));
                    }
                }
            }
            return result;
        } catch (SQLException e) {
            throw wrap("fetchChangesSince", e);
        }
    }

    private RuntimeException wrap(String op, SQLException e) {
        return new RuntimeException("rule-db sql " + op + " failed: " + e.getMessage(), e);
    }
}
```

`src/main/resources/META-INF/services/com.yomahub.liteflow.repository.RuleRepository` 内容：

```
com.yomahub.liteflow.repository.sql.SqlRuleRepository
```

- [ ] **Step 5: 编译验证**

Run: `mvn clean package -DskipTests -pl liteflow-rule-plugin/liteflow-rule-db-sql`
Expected: BUILD SUCCESS

- [ ] **Step 6: Commit**

```bash
git add liteflow-rule-plugin/pom.xml liteflow-rule-plugin/liteflow-rule-db-sql
git commit -m "feat(rule-db-sql): SqlRuleRepository + JDBC 连接管理 + DDL

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 8: SqlRulePublisher + SQL 端到端集成测试（H2）

**Files:**
- Create: `.../sql/SqlRulePublisher.java`（发布 API：事务内 UPSERT + INSERT change_log）
- Create: 测试模块 `liteflow-testcase-el/liteflow-testcase-el-rule-db-sql-springboot/`（pom、启动类、application.properties、测试类、组件）
- Modify: `liteflow-testcase-el/pom.xml`（加模块）

**Interfaces:**
- Consumes: Task 7 的 `SqlConnectionManager`/`SqlDialect`；core 的 SPI 与 runtime。
- Produces: `SqlRulePublisher.publishChain(chainId, el)` / `publishScript(ScriptRecord)` / `removeChain(chainId)` / `removeScript(nodeId)`，各返回新版本号（remove 返回 void）。

- [ ] **Step 1: 写 SqlRulePublisher（单事务：UPSERT + change_log）**

```java
package com.yomahub.liteflow.repository.sql;

import cn.hutool.crypto.SecureUtil;
import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.repository.vo.ScriptRecord;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * SQL 发布 API：单事务内 UPSERT 内容行（version+1、重算 md5）+ INSERT change_log。
 * 可独立于 FlowExecutor 使用（管理后台只依赖本 jar）。
 *
 * @author Bryan.Zhang
 * @since 2.16.1
 */
public class SqlRulePublisher {

    private final SqlConnectionManager connectionManager = new SqlConnectionManager();

    private final SqlDialect dialect = new SqlDialect();

    private String app() {
        RuleDbConfig cfg = LiteflowConfigGetter.get().getRuleDb();
        return cfg == null || cfg.getApplicationName() == null ? "default" : cfg.getApplicationName();
    }

    public long publishChain(String chainId, String el) {
        String md5 = SecureUtil.md5(el);
        try (Connection c = connectionManager.getConnection()) {
            c.setAutoCommit(false);
            try {
                long version = currentChainVersion(c, chainId) + 1;
                upsertChain(c, chainId, el, md5, version);
                insertChangeLog(c, "CHAIN", chainId, "UPSERT", version);
                c.commit();
                return version;
            } catch (SQLException e) {
                c.rollback();
                throw e;
            }
        } catch (SQLException e) {
            throw new RuntimeException("publishChain failed: " + e.getMessage(), e);
        }
    }

    public long publishScript(ScriptRecord s) {
        String md5 = SecureUtil.md5(s.getScript());
        try (Connection c = connectionManager.getConnection()) {
            c.setAutoCommit(false);
            try {
                long version = currentScriptVersion(c, s.getNodeId()) + 1;
                upsertScript(c, s, md5, version);
                insertChangeLog(c, "SCRIPT", s.getNodeId(), "UPSERT", version);
                c.commit();
                return version;
            } catch (SQLException e) {
                c.rollback();
                throw e;
            }
        } catch (SQLException e) {
            throw new RuntimeException("publishScript failed: " + e.getMessage(), e);
        }
    }

    public void removeChain(String chainId) {
        try (Connection c = connectionManager.getConnection()) {
            c.setAutoCommit(false);
            try {
                long version = currentChainVersion(c, chainId);
                try (PreparedStatement ps = c.prepareStatement(
                        "DELETE FROM " + dialect.chainTable() + " WHERE application_name = ? AND chain_id = ?")) {
                    ps.setString(1, app());
                    ps.setString(2, chainId);
                    ps.executeUpdate();
                }
                insertChangeLog(c, "CHAIN", chainId, "DELETE", version);
                c.commit();
            } catch (SQLException e) {
                c.rollback();
                throw e;
            }
        } catch (SQLException e) {
            throw new RuntimeException("removeChain failed: " + e.getMessage(), e);
        }
    }

    public void removeScript(String nodeId) {
        try (Connection c = connectionManager.getConnection()) {
            c.setAutoCommit(false);
            try {
                long version = currentScriptVersion(c, nodeId);
                try (PreparedStatement ps = c.prepareStatement(
                        "DELETE FROM " + dialect.scriptTable() + " WHERE application_name = ? AND node_id = ?")) {
                    ps.setString(1, app());
                    ps.setString(2, nodeId);
                    ps.executeUpdate();
                }
                insertChangeLog(c, "SCRIPT", nodeId, "DELETE", version);
                c.commit();
            } catch (SQLException e) {
                c.rollback();
                throw e;
            }
        } catch (SQLException e) {
            throw new RuntimeException("removeScript failed: " + e.getMessage(), e);
        }
    }

    private long currentChainVersion(Connection c, String chainId) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT version FROM " + dialect.chainTable() + " WHERE application_name = ? AND chain_id = ?")) {
            ps.setString(1, app());
            ps.setString(2, chainId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0;
            }
        }
    }

    private long currentScriptVersion(Connection c, String nodeId) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT version FROM " + dialect.scriptTable() + " WHERE application_name = ? AND node_id = ?")) {
            ps.setString(1, app());
            ps.setString(2, nodeId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0;
            }
        }
    }

    private void upsertChain(Connection c, String chainId, String el, String md5, long version) throws SQLException {
        int updated;
        try (PreparedStatement ps = c.prepareStatement("UPDATE " + dialect.chainTable()
                + " SET el_data = ?, content_md5 = ?, version = ?, enable = 1, gmt_modified = CURRENT_TIMESTAMP"
                + " WHERE application_name = ? AND chain_id = ?")) {
            ps.setString(1, el);
            ps.setString(2, md5);
            ps.setLong(3, version);
            ps.setString(4, app());
            ps.setString(5, chainId);
            updated = ps.executeUpdate();
        }
        if (updated == 0) {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO " + dialect.chainTable()
                    + " (application_name, chain_id, el_data, content_md5, version, enable) VALUES (?, ?, ?, ?, ?, 1)")) {
                ps.setString(1, app());
                ps.setString(2, chainId);
                ps.setString(3, el);
                ps.setString(4, md5);
                ps.setLong(5, version);
                ps.executeUpdate();
            }
        }
    }

    private void upsertScript(Connection c, ScriptRecord s, String md5, long version) throws SQLException {
        int updated;
        try (PreparedStatement ps = c.prepareStatement("UPDATE " + dialect.scriptTable()
                + " SET script_data = ?, script_name = ?, script_type = ?, script_language = ?, content_md5 = ?, version = ?, enable = 1, gmt_modified = CURRENT_TIMESTAMP"
                + " WHERE application_name = ? AND node_id = ?")) {
            ps.setString(1, s.getScript());
            ps.setString(2, s.getName());
            ps.setString(3, s.getType());
            ps.setString(4, s.getLanguage());
            ps.setString(5, md5);
            ps.setLong(6, version);
            ps.setString(7, app());
            ps.setString(8, s.getNodeId());
            updated = ps.executeUpdate();
        }
        if (updated == 0) {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO " + dialect.scriptTable()
                    + " (application_name, node_id, script_data, script_name, script_type, script_language, content_md5, version, enable) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)")) {
                ps.setString(1, app());
                ps.setString(2, s.getNodeId());
                ps.setString(3, s.getScript());
                ps.setString(4, s.getName());
                ps.setString(5, s.getType());
                ps.setString(6, s.getLanguage());
                ps.setString(7, md5);
                ps.setLong(8, version);
                ps.executeUpdate();
            }
        }
    }

    private void insertChangeLog(Connection c, String targetType, String targetId, String op, long version) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("INSERT INTO " + dialect.changeLogTable()
                + " (application_name, target_type, target_id, op, version) VALUES (?, ?, ?, ?, ?)")) {
            ps.setString(1, app());
            ps.setString(2, targetType);
            ps.setString(3, targetId);
            ps.setString(4, op);
            ps.setLong(5, version);
            ps.executeUpdate();
        }
    }
}
```

- [ ] **Step 2: 建 springboot 测试模块骨架**

`liteflow-testcase-el/liteflow-testcase-el-rule-db-sql-springboot/pom.xml`（依赖 liteflow-rule-db-sql + spring-boot-starter + H2 + groovy），并在 `liteflow-testcase-el/pom.xml` modules 加 `liteflow-testcase-el-rule-db-sql-springboot`。参照 `liteflow-testcase-el-sql-springboot/pom.xml` 的结构，替换插件依赖为：

```xml
        <dependency>
            <groupId>com.yomahub</groupId>
            <artifactId>liteflow-rule-db-sql</artifactId>
            <version>${revision}</version>
        </dependency>
        <dependency>
            <groupId>com.yomahub</groupId>
            <artifactId>liteflow-spring-boot-starter</artifactId>
            <version>${revision}</version>
        </dependency>
        <dependency>
            <groupId>com.h2database</groupId>
            <artifactId>h2</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>com.yomahub</groupId>
            <artifactId>liteflow-script-groovy</artifactId>
            <version>${revision}</version>
            <scope>test</scope>
        </dependency>
```

（H2、spring-boot-test 版本继承父 pom 的 dependencyManagement；若 H2 未在 dm 中，用 `liteflow-testcase-el-sql-springboot` 已用到的坐标与版本。）

`src/test/resources/application.properties`：

```properties
spring.datasource.url=jdbc:h2:mem:ruledb;DB_CLOSE_DELAY=-1;MODE=MySQL
spring.datasource.username=sa
spring.datasource.password=
spring.datasource.driver-class-name=org.h2.Driver
spring.application.name=ruledb-sql-it
liteflow.rule-db.enabled=true
liteflow.rule-db.auto-init-table=true
liteflow.rule-db.reconcile-seconds=60
liteflow.rule-db.seq-poll-seconds=1
```

> 注意：本模块依赖计划 3（starter 的 `liteflow.rule-db.*` 绑定）才能把 properties 映射进 `RuleDbConfig`。**若计划 3 尚未完成**，本测试改为在 `@BeforeEach` 用编程方式 set `RuleDbConfig`（复用容器 DataSource：通过 `datasource-bean-name=dataSource` 或直接用同一 H2 url），并在 `application.properties` 省略 `liteflow.rule-db.*`。执行顺序上建议先做计划 3 再做本 Step，或按编程式兜底。

启动类 `RuleDbSqlApplication`（标准 `@SpringBootApplication`，`@ComponentScan` 覆盖组件包）。

- [ ] **Step 3: 写端到端集成测试**

`RuleDbSqlIT.java`（核心断言：发布→执行、变更→轮询收敛、脚本发布→执行）：

```java
package com.yomahub.liteflow.test.ruledb.sql;

import com.yomahub.liteflow.core.FlowExecutor;
import com.yomahub.liteflow.flow.LiteflowResponse;
import com.yomahub.liteflow.repository.RuleDbSyncManager;
import com.yomahub.liteflow.repository.sql.SqlRulePublisher;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(classes = RuleDbSqlApplication.class)
public class RuleDbSqlIT {

    @Autowired
    private FlowExecutor flowExecutor;

    @Test
    public void testPublishThenExecuteAndConverge() {
        SqlRulePublisher publisher = new SqlRulePublisher();
        publisher.publishChain("chainA", "THEN(a, b)");
        // 触发一次对账，把新发布的 chain 纳入索引（或依赖启动时 manifest 已含——此处显式对账更稳）
        RuleDbSyncManager.reconcileOnce();

        LiteflowResponse r1 = flowExecutor.execute2Resp("chainA", "arg");
        Assertions.assertTrue(r1.isSuccess());
        Assertions.assertEquals("a==>b", r1.getExecuteStepStr());

        publisher.publishChain("chainA", "THEN(b, a)");
        RuleDbSyncManager.pollOnce();

        LiteflowResponse r2 = flowExecutor.execute2Resp("chainA", "arg");
        Assertions.assertEquals("b==>a", r2.getExecuteStepStr());
    }

    @Test
    public void testScriptPublishAndExecute() {
        SqlRulePublisher publisher = new SqlRulePublisher();
        com.yomahub.liteflow.repository.vo.ScriptRecord s = new com.yomahub.liteflow.repository.vo.ScriptRecord();
        s.setNodeId("sqlS1");
        s.setType("script");
        s.setLanguage("groovy");
        s.setScript("defaultContext.setData(\"sqlS1\", true);");
        publisher.publishScript(s);
        publisher.publishChain("chainS", "THEN(a, sqlS1)");
        RuleDbSyncManager.reconcileOnce();

        LiteflowResponse r = flowExecutor.execute2Resp("chainS", "arg");
        Assertions.assertTrue(r.isSuccess());
        Assertions.assertEquals(Boolean.TRUE,
                r.getContextBean(com.yomahub.liteflow.slot.DefaultContext.class).getData("sqlS1"));
    }
}
```

组件 `ACmp`/`BCmp`（`@Component("a")` 等，`@LiteflowCmpDefine` 或继承 NodeComponent，参照 `liteflow-testcase-el-sql-springboot` 的组件写法）。

- [ ] **Step 4: 运行集成测试**

Run: `mvn test -DskipTests=false -pl liteflow-testcase-el/liteflow-testcase-el-rule-db-sql-springboot`
Expected: PASS

排障：H2 的 `CURRENT_TIMESTAMP` 默认、`MODE=MySQL` 兼容、`enable = 1` 的 TINYINT 比较若报错，调整 ddl-h2.sql 的类型（H2 用 `BOOLEAN` 或 `INT`）。DDL 与查询里的 `enable` 语义要一致。

- [ ] **Step 5: Commit**

```bash
git add liteflow-rule-plugin/liteflow-rule-db-sql liteflow-testcase-el/pom.xml liteflow-testcase-el/liteflow-testcase-el-rule-db-sql-springboot
git commit -m "feat(rule-db-sql): SqlRulePublisher + H2 端到端集成测试

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## 计划 1 完成校验

全部任务完成后运行：

```bash
mvn clean package -DskipTests -pl liteflow-core,liteflow-rule-plugin/liteflow-rule-db-sql
mvn test -DskipTests=false -pl liteflow-testcase-el/liteflow-testcase-el-rule-db-core,liteflow-testcase-el/liteflow-testcase-el-rule-db-sql-springboot
```

预期：BUILD SUCCESS + 所有 rule-db 测试绿。

**交接给计划 2（redis 插件）与计划 3（starter 绑定 + metadata + 文档）：**
- 计划 3 必须实现 `liteflow.rule-db.*` → `RuleDbConfig` 的 Spring Boot 2/3/4 与 Solon 绑定，并生成 `additional-spring-configuration-metadata.json`，否则 Task 8 的 properties 驱动方式需用编程式兜底。
- 计划 2 的 redis 插件复用 core 的全部 runtime，仅需实现 `RedisRuleRepository`（含 `subscribe` 推送）与 `RedisRulePublisher`（Lua 原子发布），测试模块 `liteflow-testcase-el-rule-db-redis-springboot`。

