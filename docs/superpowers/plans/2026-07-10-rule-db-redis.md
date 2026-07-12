# Rule-DB 模式（Redis 插件）实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 `liteflow-rule-db-redis` 插件——Redis 作为规则/脚本权威源，复用计划 1 的 core 运行时（索引/缓存/同步/对账），提供 pub/sub 低延迟推送 + ZSet 变更日志兜底 + Lua 原子发布。

**Architecture:** `RedisRuleRepository` 实现 core 的 `RuleRepository` SPI（ServiceLoader 装载）；Redisson 承载连接（single/sentinel/cluster 自动识别）；变更日志用 ZSet（score=seq）实现 `fetchChangesSince`/断档检测，与 SQL 的 `change_log` 语义对齐；`subscribe` 走 RTopic 推送。规格见 `docs/superpowers/specs/2026-07-10-rule-db-plugin-design.md`。

**Tech Stack:** JDK 8 语法、Redisson、hutool-crypto、Jackson、embedded-redis（测试）、Spring Boot 2 starter。

**前置：计划 1 已完成**（core 的 `RuleRepository`/`RuleDbRuntime`/`RuleDbSyncManager`/`RuleDbConfig` 均已就位）。本计划仅新增一个插件模块 + 一个测试模块，不改 core。

## Global Constraints

- **JDK 8 语法**，插件在 `compile-8-to-16` profile 下构建。
- **测试只放 `liteflow-testcase-el/` 下**；核心/插件模块内不放测试。
- **运行测试必须 `-DskipTests=false`**，禁止改根 pom surefire 默认。
- 版本用 `${revision}`，不写死。
- 新模块 `liteflow-rule-db-redis` 挂在根级独立父模块 `liteflow-rule-db` 的聚合 pom 下；该父模块已由根 pom 的两个 compile profile 聚合。
- 提交信息中文、conventional-commits，结尾 `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`。
- Redisson 依赖照 `liteflow-rule-redis/pom.xml` 的坐标与 exclusions 复制（jackson 冲突排除一致）。
- 每个 VO/类补全标准 getter/setter（IDE 生成），不是可省项。

## 设计细化（本计划落实，回写 spec §6.2 与 §7）

Redis 键结构在设计 §6.2 基础上补一个变更日志键（原 §6.2 只列了 `seq` 与 `notify`，`fetchChangesSince` 需要可按 seq 范围查询且可裁剪的日志）：

```
lf:{app}:chain:{chainId}    HASH   el / route / namespace / version / md5 / enable
lf:{app}:script:{nodeId}    HASH   script / name / type / language / version / md5 / enable
lf:{app}:chain-index        HASH   chainId → "version|md5"
lf:{app}:script-index       HASH   nodeId  → "version|md5|type|language|name"
lf:{app}:seq                STRING （INCR）
lf:{app}:changelog          ZSET   score=seq, member=JSON{seq,targetType,targetId,op,version}   ← 新增
lf:{app}:notify             pub/sub channel，消息 = 同上 JSON
```

- `fetchChangesSince(seq)` = `ZRANGEBYSCORE changelog (seq +inf`；断档检测 = ZSet 最小 score > seq+1（日志被裁剪）→ 抛 `SeqGapException`。
- `subscribe` = RTopic 订阅 notify channel，消息即一条 `ChangeRecord`。
- 发布 = 一段 Lua 原子完成：HSET 内容 → HSET index → INCR seq → ZADD changelog → PUBLISH notify。
- 裁剪由运维用 `ZREMRANGEBYSCORE` 或按大小裁剪，不影响正确性（断档触发对账）。

---

### Task 1: 模块骨架 + Redisson 连接管理

**Files:**
- Create: `liteflow-rule-db/liteflow-rule-db-redis/pom.xml`
- Modify: `liteflow-rule-db/pom.xml`（modules 加 `liteflow-rule-db-redis`）
- Create: `.../repository/redis/RedisConnectionManager.java`
- Create: `.../repository/redis/RedisKeys.java`（键名拼装，前缀+app 可配）

**Interfaces:**
- Consumes: core 的 `LiteflowConfigGetter.get().getRuleDb()`、`ContextAwareHolder`。
- Produces:
  - `RedisConnectionManager.getClient()` : `RedissonClient`（懒建，优先复用容器 bean）
  - `RedisConnectionManager.shutdown()` : void
  - `RedisKeys.chain(chainId)` / `script(nodeId)` / `chainIndex()` / `scriptIndex()` / `seq()` / `changelog()` / `notify()` : String

- [ ] **Step 1: 建 pom 并挂聚合**

`liteflow-rule-db/liteflow-rule-db-redis/pom.xml`：

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <parent>
        <artifactId>liteflow-rule-db</artifactId>
        <groupId>com.yomahub</groupId>
        <version>${revision}</version>
        <relativePath>../pom.xml</relativePath>
    </parent>
    <modelVersion>4.0.0</modelVersion>
    <artifactId>liteflow-rule-db-redis</artifactId>
    <name>${project.artifactId}</name>
    <dependencies>
        <dependency>
            <groupId>com.yomahub</groupId>
            <artifactId>liteflow-core</artifactId>
            <version>${revision}</version>
        </dependency>
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
        </dependency>
        <dependency>
            <groupId>org.redisson</groupId>
            <artifactId>redisson</artifactId>
            <exclusions>
                <exclusion>
                    <groupId>com.fasterxml.jackson.core</groupId>
                    <artifactId>jackson-databind</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>com.fasterxml.jackson.core</groupId>
                    <artifactId>jackson-core</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
        <dependency>
            <groupId>cn.hutool</groupId>
            <artifactId>hutool-crypto</artifactId>
        </dependency>
    </dependencies>
</project>
```

`liteflow-rule-db/pom.xml` 的 `<modules>` 在 `liteflow-rule-db-sql` 后加 `<module>liteflow-rule-db-redis</module>`。

- [ ] **Step 2: 写 RedisKeys**

```java
package com.yomahub.liteflow.repository.redis;

import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbConfig;

/**
 * Redis 键名拼装：{prefix}:{app}:xxx。
 *
 * @author Bryan.Zhang
 * @since 2.16.1
 */
public class RedisKeys {

    private static String prefix() {
        RuleDbConfig cfg = LiteflowConfigGetter.get().getRuleDb();
        String p = cfg == null ? null : cfg.getKeyPrefix();
        return StrUtil.isBlank(p) ? "lf" : p;
    }

    private static String app() {
        RuleDbConfig cfg = LiteflowConfigGetter.get().getRuleDb();
        String a = cfg == null ? null : cfg.getApplicationName();
        return StrUtil.isBlank(a) ? "default" : a;
    }

    private static String base() {
        return prefix() + ":" + app();
    }

    public static String chain(String chainId) {
        return base() + ":chain:" + chainId;
    }

    public static String script(String nodeId) {
        return base() + ":script:" + nodeId;
    }

    public static String chainIndex() {
        return base() + ":chain-index";
    }

    public static String scriptIndex() {
        return base() + ":script-index";
    }

    public static String seq() {
        return base() + ":seq";
    }

    public static String changelog() {
        return base() + ":changelog";
    }

    public static String notifyChannel() {
        return base() + ":notify";
    }
}
```

- [ ] **Step 3: 写 RedisConnectionManager（single/sentinel/cluster 自动识别 + 容器 bean 复用）**

```java
package com.yomahub.liteflow.repository.redis;

import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.spi.holder.ContextAwareHolder;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

/**
 * Redisson 连接：优先复用容器 RedissonClient（redisson-bean-name 指定或按类型），
 * 否则用 address（+ master-name）自动识别 single/sentinel/cluster 自建。
 *
 * @author Bryan.Zhang
 * @since 2.16.1
 */
public class RedisConnectionManager {

    private volatile RedissonClient client;

    private volatile boolean selfCreated = false;

    public RedissonClient getClient() {
        if (client != null) {
            return client;
        }
        synchronized (this) {
            if (client != null) {
                return client;
            }
            RuleDbConfig cfg = LiteflowConfigGetter.get().getRuleDb();
            RedissonClient bean = lookupBean(cfg);
            if (bean != null) {
                client = bean;
                selfCreated = false;
                return client;
            }
            if (StrUtil.isBlank(cfg.getAddress())) {
                throw new ConfigErrorException("rule-db redis: neither a RedissonClient bean nor liteflow.rule-db.address is available");
            }
            client = Redisson.create(buildConfig(cfg));
            selfCreated = true;
            return client;
        }
    }

    private RedissonClient lookupBean(RuleDbConfig cfg) {
        try {
            if (StrUtil.isNotBlank(cfg.getRedissonBeanName())) {
                return ContextAwareHolder.loadContextAware().getBean(cfg.getRedissonBeanName());
            }
            return ContextAwareHolder.loadContextAware().getBean(RedissonClient.class);
        } catch (Exception e) {
            return null;
        }
    }

    private Config buildConfig(RuleDbConfig cfg) {
        Config config = new Config();
        String[] addresses = cfg.getAddress().split(",");
        int db = cfg.getDatabase() == null ? 0 : cfg.getDatabase();
        if (addresses.length > 1) {
            if (StrUtil.isNotBlank(cfg.getMasterName())) {
                // 哨兵
                org.redisson.config.SentinelServersConfig s = config.useSentinelServers()
                        .setMasterName(cfg.getMasterName())
                        .setDatabase(db);
                for (String a : addresses) {
                    s.addSentinelAddress(normalize(a));
                }
                applyAuth(cfg, s.setPassword(cfg.getPassword()) == null ? null : null); // 见下 applyAuth
                if (StrUtil.isNotBlank(cfg.getPassword())) {
                    s.setPassword(cfg.getPassword());
                }
                if (StrUtil.isNotBlank(cfg.getUsername())) {
                    s.setUsername(cfg.getUsername());
                }
            } else {
                // 集群
                org.redisson.config.ClusterServersConfig c = config.useClusterServers();
                for (String a : addresses) {
                    c.addNodeAddress(normalize(a));
                }
                if (StrUtil.isNotBlank(cfg.getPassword())) {
                    c.setPassword(cfg.getPassword());
                }
                if (StrUtil.isNotBlank(cfg.getUsername())) {
                    c.setUsername(cfg.getUsername());
                }
            }
        } else {
            // 单机
            org.redisson.config.SingleServerConfig single = config.useSingleServer()
                    .setAddress(normalize(addresses[0]))
                    .setDatabase(db);
            if (StrUtil.isNotBlank(cfg.getPassword())) {
                single.setPassword(cfg.getPassword());
            }
            if (StrUtil.isNotBlank(cfg.getUsername())) {
                single.setUsername(cfg.getUsername());
            }
        }
        return config;
    }

    /** 允许用户写 host:port 或 redis://host:port，统一补协议 */
    private String normalize(String address) {
        String a = address.trim();
        return a.startsWith("redis://") || a.startsWith("rediss://") ? a : "redis://" + a;
    }

    private void applyAuth(RuleDbConfig cfg, Object ignored) {
        // 占位：真正的鉴权在各分支内 setPassword/setUsername 完成；此方法保留空实现以聚焦分支可读性
    }

    public synchronized void shutdown() {
        if (client != null && selfCreated) {
            client.shutdown();
        }
        client = null;
        selfCreated = false;
    }
}
```

> 清理注记：`buildConfig` 的哨兵分支里那行 `applyAuth(... s.setPassword(...) ...)` 是笔误，实现时删掉该行与 `applyAuth` 方法，只保留分支内显式的 `setPassword/setUsername`。（保留在此是为提示实现者按最终形态精简；参照 `liteflow-rule-redis` 的 `RedisParserHelper.getSingleRedissonConfig` 等方法的鉴权写法对齐。）

- [ ] **Step 4: 编译验证**

Run: `mvn clean package -DskipTests -pl liteflow-rule-db/liteflow-rule-db-redis`
Expected: BUILD SUCCESS

- [ ] **Step 5: Commit**

```bash
git add liteflow-rule-db/pom.xml liteflow-rule-db/liteflow-rule-db-redis
git commit -m "feat(rule-db-redis): 模块骨架 + Redisson 连接管理（single/sentinel/cluster 自动识别）

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: RedisRuleRepository（读取 + ZSet 变更日志 + pub/sub 订阅）

**Files:**
- Create: `.../repository/redis/RedisRuleRepository.java`
- Create: `.../repository/redis/ChangeCodec.java`（ChangeRecord ↔ JSON）
- Create: `liteflow-rule-db/liteflow-rule-db-redis/src/main/resources/META-INF/services/com.yomahub.liteflow.repository.RuleRepository`

**Interfaces:**
- Consumes: Task 1 的 `RedisConnectionManager`/`RedisKeys`；core VO 与 SPI。
- Produces: `RedisRuleRepository`（无参构造，ServiceLoader 装载）；`ChangeCodec.toJson(ChangeRecord)` / `fromJson(String)`。

- [ ] **Step 1: 写 ChangeCodec**

```java
package com.yomahub.liteflow.repository.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yomahub.liteflow.repository.vo.ChangeRecord;

/**
 * ChangeRecord ↔ JSON。字段：seq/targetType/targetId/op/version。
 *
 * @author Bryan.Zhang
 * @since 2.16.1
 */
public class ChangeCodec {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static String toJson(ChangeRecord c) {
        try {
            return MAPPER.writeValueAsString(c);
        } catch (Exception e) {
            throw new RuntimeException("encode ChangeRecord failed: " + e.getMessage(), e);
        }
    }

    public static ChangeRecord fromJson(String json) {
        try {
            return MAPPER.readValue(json, ChangeRecord.class);
        } catch (Exception e) {
            throw new RuntimeException("decode ChangeRecord failed: " + json, e);
        }
    }
}
```

> 前置：`ChangeRecord`（计划 1 Task 1）需能被 Jackson 序列化——它已有无参构造 + 全字段 getter/setter，枚举 `TargetType`/`Op` 默认按名序列化，无需额外注解。若反序列化枚举报错，给 `ChangeRecord` 加无参构造已足够（已具备）。

- [ ] **Step 2: 写 RedisRuleRepository**

```java
package com.yomahub.liteflow.repository.redis;

import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.exception.SeqGapException;
import com.yomahub.liteflow.repository.RuleChangeListener;
import com.yomahub.liteflow.repository.RuleRepository;
import com.yomahub.liteflow.repository.vo.*;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Rule-DB 的 Redis 权威源实现。
 *
 * @author Bryan.Zhang
 * @since 2.16.1
 */
public class RedisRuleRepository implements RuleRepository {

    private final RedisConnectionManager connectionManager = new RedisConnectionManager();

    private RedissonClient redisson() {
        return connectionManager.getClient();
    }

    @Override
    public RuleManifest fetchManifest() {
        RedissonClient client = redisson();
        RuleManifest manifest = new RuleManifest();
        List<ChainMeta> chains = new ArrayList<>();
        Map<String, String> chainIndex = client.getMap(RedisKeys.chainIndex(), StringCodec.INSTANCE).readAllMap();
        for (Map.Entry<String, String> e : chainIndex.entrySet()) {
            // value = "version|md5"
            String[] parts = e.getValue().split("\\|", -1);
            chains.add(new ChainMeta(e.getKey(), parseLong(parts, 0), get(parts, 1)));
        }
        List<ScriptMeta> scripts = new ArrayList<>();
        Map<String, String> scriptIndex = client.getMap(RedisKeys.scriptIndex(), StringCodec.INSTANCE).readAllMap();
        for (Map.Entry<String, String> e : scriptIndex.entrySet()) {
            // value = "version|md5|type|language|name"
            String[] parts = e.getValue().split("\\|", -1);
            scripts.add(new ScriptMeta(e.getKey(), parseLong(parts, 0), get(parts, 1),
                    get(parts, 2), get(parts, 3), get(parts, 4)));
        }
        manifest.setChains(chains);
        manifest.setScripts(scripts);
        manifest.setLatestSeq(fetchLatestSeq());
        return manifest;
    }

    @Override
    public ChainRecord fetchChain(String chainId) {
        Map<String, String> h = redisson().getMap(RedisKeys.chain(chainId), StringCodec.INSTANCE).readAllMap();
        if (h == null || h.isEmpty()) {
            return null;
        }
        ChainRecord r = new ChainRecord();
        r.setChainId(chainId);
        r.setEl(h.get("el"));
        r.setRoute(h.get("route"));
        r.setNamespace(h.get("namespace"));
        r.setVersion(parseLong(h.get("version")));
        r.setMd5(h.get("md5"));
        r.setEnable(!"0".equals(h.get("enable")));
        return r;
    }

    @Override
    public ScriptRecord fetchScript(String nodeId) {
        Map<String, String> h = redisson().getMap(RedisKeys.script(nodeId), StringCodec.INSTANCE).readAllMap();
        if (h == null || h.isEmpty()) {
            return null;
        }
        ScriptRecord r = new ScriptRecord();
        r.setNodeId(nodeId);
        r.setScript(h.get("script"));
        r.setName(h.get("name"));
        r.setType(h.get("type"));
        r.setLanguage(h.get("language"));
        r.setVersion(parseLong(h.get("version")));
        r.setMd5(h.get("md5"));
        r.setEnable(!"0".equals(h.get("enable")));
        return r;
    }

    @Override
    public long fetchLatestSeq() {
        Object v = redisson().getBucket(RedisKeys.seq(), StringCodec.INSTANCE).get();
        return v == null ? 0 : parseLong(v.toString());
    }

    @Override
    public List<ChangeRecord> fetchChangesSince(long seq) {
        RScoredSortedSet<String> log = redisson().getScoredSortedSet(RedisKeys.changelog(), StringCodec.INSTANCE);
        // 断档检测：最小 score > seq+1 说明中间被裁剪
        Collection<ScoredEntry<String>> firstEntry = log.entryRange(0, 0);
        if (seq > 0 && !firstEntry.isEmpty()) {
            double min = firstEntry.iterator().next().getScore();
            if (min > seq + 1) {
                throw new SeqGapException("redis changelog gap: since=" + seq + " min=" + (long) min);
            }
        }
        List<ChangeRecord> result = new ArrayList<>();
        // (seq, +inf]：exclusive 下界
        Collection<String> members = log.valueRange(seq, false, Double.POSITIVE_INFINITY, true);
        for (String json : members) {
            result.add(ChangeCodec.fromJson(json));
        }
        return result;
    }

    @Override
    public void subscribe(RuleChangeListener listener) {
        RTopic topic = redisson().getTopic(RedisKeys.notifyChannel(), StringCodec.INSTANCE);
        topic.addListener(String.class, (channel, msg) -> {
            ChangeRecord c = ChangeCodec.fromJson(msg);
            List<ChangeRecord> one = new ArrayList<>();
            one.add(c);
            listener.onChanges(one);
        });
    }

    @Override
    public void close() {
        connectionManager.shutdown();
    }

    private static long parseLong(String s) {
        return StrUtil.isBlank(s) ? 0 : Long.parseLong(s.trim());
    }

    private static long parseLong(String[] parts, int idx) {
        return idx < parts.length ? parseLong(parts[idx]) : 0;
    }

    private static String get(String[] parts, int idx) {
        String v = idx < parts.length ? parts[idx] : null;
        return StrUtil.isBlank(v) ? null : v;
    }
}
```

> import 说明：`ScoredEntry` 位于 `org.redisson.client.protocol.ScoredEntry`，`valueRange(startScore, startInclusive, endScore, endInclusive)` 与 `entryRange(startIndex, endIndex)` 为 Redisson `RScoredSortedSet` API。实现时若签名与所用 Redisson 版本不符，以 IDE 补全为准（core 使用的 Redisson 版本见根 pom dependencyManagement）。

`src/main/resources/META-INF/services/com.yomahub.liteflow.repository.RuleRepository`：

```
com.yomahub.liteflow.repository.redis.RedisRuleRepository
```

- [ ] **Step 3: 编译验证**

Run: `mvn clean package -DskipTests -pl liteflow-rule-db/liteflow-rule-db-redis`
Expected: BUILD SUCCESS

- [ ] **Step 4: Commit**

```bash
git add liteflow-rule-db/liteflow-rule-db-redis
git commit -m "feat(rule-db-redis): RedisRuleRepository（索引读取+ZSet变更日志+pub/sub订阅）

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: RedisRulePublisher（Lua 原子发布）

**Files:**
- Create: `.../repository/redis/RedisRulePublisher.java`
- Create: `liteflow-rule-db/liteflow-rule-db-redis/src/main/resources/lua/publish-chain.lua` 与 `publish-script.lua`、`remove.lua`

**Interfaces:**
- Consumes: Task 1/2。
- Produces: `RedisRulePublisher.publishChain(chainId, el)` / `publishScript(ScriptRecord)` / `removeChain(chainId)` / `removeScript(nodeId)`，chain/script 发布返回新版本号。

- [ ] **Step 1: 写 Lua 脚本**

`src/main/resources/lua/publish-chain.lua`（KEYS: chainKey, chainIndexKey, seqKey, changelogKey, notifyKey；ARGV: chainId, el, route, namespace, md5, targetType='CHAIN', op='UPSERT'）：

```lua
local chainKey = KEYS[1]
local indexKey = KEYS[2]
local seqKey = KEYS[3]
local changelogKey = KEYS[4]
local notifyKey = KEYS[5]

local chainId = ARGV[1]
local el = ARGV[2]
local route = ARGV[3]
local namespace = ARGV[4]
local md5 = ARGV[5]

local oldVersion = tonumber(redis.call('HGET', chainKey, 'version')) or 0
local version = oldVersion + 1

redis.call('HSET', chainKey, 'el', el, 'route', route, 'namespace', namespace,
    'version', version, 'md5', md5, 'enable', '1')
redis.call('HSET', indexKey, chainId, version .. '|' .. md5)

local seq = redis.call('INCR', seqKey)
local change = cjson.encode({seq = seq, targetType = 'CHAIN', targetId = chainId, op = 'UPSERT', version = version})
redis.call('ZADD', changelogKey, seq, change)
redis.call('PUBLISH', notifyKey, change)
return version
```

`publish-script.lua`（KEYS 同构，index value 为 `version|md5|type|language|name`；ARGV: nodeId, script, name, type, language, md5）：

```lua
local scriptKey = KEYS[1]
local indexKey = KEYS[2]
local seqKey = KEYS[3]
local changelogKey = KEYS[4]
local notifyKey = KEYS[5]

local nodeId = ARGV[1]
local script = ARGV[2]
local name = ARGV[3]
local ntype = ARGV[4]
local language = ARGV[5]
local md5 = ARGV[6]

local oldVersion = tonumber(redis.call('HGET', scriptKey, 'version')) or 0
local version = oldVersion + 1

redis.call('HSET', scriptKey, 'script', script, 'name', name, 'type', ntype,
    'language', language, 'version', version, 'md5', md5, 'enable', '1')
redis.call('HSET', indexKey, nodeId, version .. '|' .. md5 .. '|' .. ntype .. '|' .. language .. '|' .. name)

local seq = redis.call('INCR', seqKey)
local change = cjson.encode({seq = seq, targetType = 'SCRIPT', targetId = nodeId, op = 'UPSERT', version = version})
redis.call('ZADD', changelogKey, seq, change)
redis.call('PUBLISH', notifyKey, change)
return version
```

`remove.lua`（KEYS: contentKey, indexKey, seqKey, changelogKey, notifyKey；ARGV: targetType, targetId）：

```lua
local contentKey = KEYS[1]
local indexKey = KEYS[2]
local seqKey = KEYS[3]
local changelogKey = KEYS[4]
local notifyKey = KEYS[5]

local targetType = ARGV[1]
local targetId = ARGV[2]

local version = tonumber(redis.call('HGET', contentKey, 'version')) or 0
redis.call('DEL', contentKey)
redis.call('HDEL', indexKey, targetId)

local seq = redis.call('INCR', seqKey)
local change = cjson.encode({seq = seq, targetType = targetType, targetId = targetId, op = 'DELETE', version = version})
redis.call('ZADD', changelogKey, seq, change)
redis.call('PUBLISH', notifyKey, change)
return seq
```

- [ ] **Step 2: 写 RedisRulePublisher**

```java
package com.yomahub.liteflow.repository.redis;

import cn.hutool.core.io.resource.ResourceUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.crypto.SecureUtil;
import com.yomahub.liteflow.repository.vo.ScriptRecord;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Redis 发布 API：Lua 原子完成 HSET 内容 + HSET index + INCR seq + ZADD changelog + PUBLISH。
 * 可独立于 FlowExecutor 使用。
 *
 * @author Bryan.Zhang
 * @since 2.16.1
 */
public class RedisRulePublisher {

    private final RedisConnectionManager connectionManager = new RedisConnectionManager();

    private final String publishChainLua = ResourceUtil.readUtf8Str("lua/publish-chain.lua");
    private final String publishScriptLua = ResourceUtil.readUtf8Str("lua/publish-script.lua");
    private final String removeLua = ResourceUtil.readUtf8Str("lua/remove.lua");

    private RScript script() {
        RedissonClient client = connectionManager.getClient();
        return client.getScript(StringCodec.INSTANCE);
    }

    public long publishChain(String chainId, String el) {
        return publishChain(chainId, el, "", "");
    }

    public long publishChain(String chainId, String el, String route, String namespace) {
        List<Object> keys = Arrays.asList(RedisKeys.chain(chainId), RedisKeys.chainIndex(),
                RedisKeys.seq(), RedisKeys.changelog(), RedisKeys.notifyChannel());
        Long version = script().eval(RScript.Mode.READ_WRITE, publishChainLua, RScript.ReturnType.INTEGER,
                keys, chainId, el, nz(route), nz(namespace), SecureUtil.md5(el));
        return version;
    }

    public long publishScript(ScriptRecord s) {
        List<Object> keys = Arrays.asList(RedisKeys.script(s.getNodeId()), RedisKeys.scriptIndex(),
                RedisKeys.seq(), RedisKeys.changelog(), RedisKeys.notifyChannel());
        Long version = script().eval(RScript.Mode.READ_WRITE, publishScriptLua, RScript.ReturnType.INTEGER,
                keys, s.getNodeId(), s.getScript(), nz(s.getName()), nz(s.getType()),
                nz(s.getLanguage()), SecureUtil.md5(s.getScript()));
        return version;
    }

    public void removeChain(String chainId) {
        List<Object> keys = Arrays.asList(RedisKeys.chain(chainId), RedisKeys.chainIndex(),
                RedisKeys.seq(), RedisKeys.changelog(), RedisKeys.notifyChannel());
        script().eval(RScript.Mode.READ_WRITE, removeLua, RScript.ReturnType.INTEGER, keys, "CHAIN", chainId);
    }

    public void removeScript(String nodeId) {
        List<Object> keys = Arrays.asList(RedisKeys.script(nodeId), RedisKeys.scriptIndex(),
                RedisKeys.seq(), RedisKeys.changelog(), RedisKeys.notifyChannel());
        script().eval(RScript.Mode.READ_WRITE, removeLua, RScript.ReturnType.INTEGER, keys, "SCRIPT", nodeId);
    }

    private static String nz(String s) {
        return StrUtil.isBlank(s) ? "" : s;
    }
}
```

> 说明：`RScript.eval(Mode, luaText, ReturnType, keys, values...)` 的 keys 需为 `List<Object>`，values 为可变参。返回 `INTEGER` 映射为 `Long`。若所用 Redisson 版本 eval 签名不同（部分版本为 `eval(mode, lua, returnType, List keys, Object... values)`），以 IDE 补全为准，保持 keys=5 个、values 顺序与 Lua 的 ARGV 对齐。cjson 为 Redis 内置，Lua 中可直接用。

- [ ] **Step 3: 编译验证**

Run: `mvn clean package -DskipTests -pl liteflow-rule-db/liteflow-rule-db-redis`
Expected: BUILD SUCCESS

- [ ] **Step 4: Commit**

```bash
git add liteflow-rule-db/liteflow-rule-db-redis
git commit -m "feat(rule-db-redis): RedisRulePublisher（Lua 原子发布：内容+索引+seq+changelog+notify）

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: Redis 端到端集成测试（embedded-redis）

**Files:**
- Create: 测试模块 `liteflow-testcase-el/liteflow-testcase-el-rule-db-redis-springboot/`（pom、启动类、application.properties、组件、IT）
- Modify: `liteflow-testcase-el/pom.xml`（加模块）

**Interfaces:**
- Consumes: 插件全部 + core runtime。
- Produces: 无（测试终点）。

- [ ] **Step 1: 建测试模块 pom（含 embedded-redis）**

`pom.xml`（参照 `liteflow-testcase-el-rule-db-sql-springboot`，替换插件依赖 + 加 embedded-redis）：

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
    <artifactId>liteflow-testcase-el-rule-db-redis-springboot</artifactId>
    <dependencies>
        <dependency>
            <groupId>com.yomahub</groupId>
            <artifactId>liteflow-spring-boot-starter</artifactId>
            <version>${revision}</version>
        </dependency>
        <dependency>
            <groupId>com.yomahub</groupId>
            <artifactId>liteflow-rule-db-redis</artifactId>
            <version>${revision}</version>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
        </dependency>
        <dependency>
            <groupId>com.yomahub</groupId>
            <artifactId>liteflow-script-groovy</artifactId>
            <version>${revision}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>com.github.codemonstur</groupId>
            <artifactId>embedded-redis</artifactId>
            <version>1.4.3</version>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>
```

> embedded-redis 说明：`com.github.codemonstur:embedded-redis`（`redis.embedded.RedisServer`）是维护中的嵌入式 redis，Java 8 兼容、支持单机，足够验证 Redisson 单机路径。若该坐标在本机 maven 仓库不可用，回退用 `it.ozimov:embedded-redis:0.7.3`（包名同为 `redis.embedded`，API 兼容）。若两者都无法拉取，则本 IT 用 `@EnabledIfEnvironmentVariable(named = "REDIS_HOST", matches = ".+")` 门控，改连外部 redis。

- [ ] **Step 2: 写启动类 + embedded redis 生命周期 + properties**

`RuleDbRedisApplication`：标准 `@SpringBootApplication`。

`application.properties`：

```properties
spring.application.name=ruledb-redis-it
liteflow.rule-db.enabled=true
liteflow.rule-db.address=redis://127.0.0.1:16379
liteflow.rule-db.seq-poll-seconds=1
liteflow.rule-db.reconcile-seconds=60
```

IT 里用 `@BeforeAll` 启动 `RedisServer.newRedisServer().port(16379).build()`，`@AfterAll` 停止。

> 同 Task 8（计划 1）：`liteflow.rule-db.*` 的 properties 绑定依赖计划 3。若计划 3 未完成，改在 `@BeforeEach`/`@BeforeAll` 编程式构造 `RuleDbConfig`（set address/applicationName/seqPollSeconds）并塞进 `LiteflowConfig`。执行顺序建议先做计划 3。

- [ ] **Step 3: 写 IT（pub/sub 推送收敛 + poll 收敛 + 脚本）**

`RuleDbRedisIT.java`：

```java
package com.yomahub.liteflow.test.ruledb.redis;

import com.yomahub.liteflow.core.FlowExecutor;
import com.yomahub.liteflow.flow.LiteflowResponse;
import com.yomahub.liteflow.repository.RuleDbSyncManager;
import com.yomahub.liteflow.repository.redis.RedisRulePublisher;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import redis.embedded.RedisServer;

import java.io.IOException;

@SpringBootTest(classes = RuleDbRedisApplication.class)
public class RuleDbRedisIT {

    private static RedisServer redisServer;

    @Autowired
    private FlowExecutor flowExecutor;

    @BeforeAll
    public static void startRedis() throws IOException {
        redisServer = RedisServer.newRedisServer().port(16379).build();
        redisServer.start();
    }

    @AfterAll
    public static void stopRedis() throws IOException {
        if (redisServer != null) {
            redisServer.stop();
        }
    }

    @Test
    public void testPublishExecuteAndConverge() {
        RedisRulePublisher publisher = new RedisRulePublisher();
        publisher.publishChain("rchainA", "THEN(a, b)");
        RuleDbSyncManager.reconcileOnce();

        LiteflowResponse r1 = flowExecutor.execute2Resp("rchainA", "arg");
        Assertions.assertTrue(r1.isSuccess());
        Assertions.assertEquals("a==>b", r1.getExecuteStepStr());

        publisher.publishChain("rchainA", "THEN(b, a)");
        RuleDbSyncManager.pollOnce(); // 或等待 pub/sub 推送

        LiteflowResponse r2 = flowExecutor.execute2Resp("rchainA", "arg");
        Assertions.assertEquals("b==>a", r2.getExecuteStepStr());
    }

    @Test
    public void testScriptPublishAndExecute() {
        RedisRulePublisher publisher = new RedisRulePublisher();
        com.yomahub.liteflow.repository.vo.ScriptRecord s = new com.yomahub.liteflow.repository.vo.ScriptRecord();
        s.setNodeId("rS1");
        s.setType("script");
        s.setLanguage("groovy");
        s.setScript("defaultContext.setData(\"rS1\", true);");
        publisher.publishScript(s);
        publisher.publishChain("rchainS", "THEN(a, rS1)");
        RuleDbSyncManager.reconcileOnce();

        LiteflowResponse r = flowExecutor.execute2Resp("rchainS", "arg");
        Assertions.assertTrue(r.isSuccess());
        Assertions.assertEquals(Boolean.TRUE,
                r.getContextBean(com.yomahub.liteflow.slot.DefaultContext.class).getData("rS1"));
    }
}
```

组件 `ACmp`/`BCmp`（`@Component("a")` / `@Component("b")` 继承 NodeComponent，输出到 executeStep）。启动类需 `@ComponentScan` 覆盖组件包。

- [ ] **Step 4: 运行集成测试**

Run: `mvn test -DskipTests=false -pl liteflow-testcase-el/liteflow-testcase-el-rule-db-redis-springboot`
Expected: PASS

排障：embedded-redis 若因端口占用/二进制不兼容启动失败，换端口或用外部 redis 门控（见 Step 1）。Lua `cjson` 在 embedded-redis 内建可用；若不可用则改用手工字符串拼 JSON（member 用固定字段顺序）。

- [ ] **Step 5: 更新 spec 的 §6.2/§7 记录 changelog ZSet**

在 `docs/superpowers/specs/2026-07-10-rule-db-plugin-design.md` §6.2 的键结构表加一行 `changelog ZSET score=seq`，并在 §7 注明 Redis 发布用 Lua 原子完成"内容+索引+seq+changelog+notify"。

- [ ] **Step 6: Commit**

```bash
git add liteflow-testcase-el/pom.xml liteflow-testcase-el/liteflow-testcase-el-rule-db-redis-springboot docs/superpowers/specs/2026-07-10-rule-db-plugin-design.md
git commit -m "test(rule-db-redis): embedded-redis 端到端集成 + spec 补 changelog ZSet

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## 计划 2 完成校验

```bash
mvn clean package -DskipTests -pl liteflow-rule-db/liteflow-rule-db-redis
mvn test -DskipTests=false -pl liteflow-testcase-el/liteflow-testcase-el-rule-db-redis-springboot
```

预期：BUILD SUCCESS + redis 集成测试绿（或在无 redis 环境按门控跳过）。

**依赖关系：** 本计划的 properties 驱动测试依赖计划 3（starter 绑定）。若先于计划 3 执行，Task 4 用编程式 `RuleDbConfig` 兜底。
