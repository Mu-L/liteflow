# Rule-DB 模式设计：规则/脚本以存储引擎为权威源

日期：2026-07-10
状态：待评审
落点：`liteflow-core`（新增 SPI 与运行时设施）+ 两个全新插件模块 `liteflow-rule-db-sql`、`liteflow-rule-db-redis`

---

## 1. 背景与问题

LiteFlow 现有的 6 个规则存储插件（sql/redis/zk/nacos/etcd/apollo）本质相同：启动时从存储全量读出规则和脚本，拼成一个大 XML，走 `LocalXmlFlowELParser` 同一条解析路径。由此产生两个本质问题：

1. **多节点无一致性保证。** 存储只是"启动数据源"，真正的规则活在各节点 JVM 内。刷新依赖各插件自身的轮询或通知机制，时间窗内各节点版本不一，且通知丢失后没有兜底对账，无法保证收敛。
2. **规则与脚本全量常驻 JVM 堆。** `PARSE_ONE_ON_FIRST_EXEC` 只推迟编译（条件树构建、脚本编译），EL 文本和脚本源码依然全量驻留 `FlowBus.chainMap / nodeMap`；`chainCacheEnabled` 只淘汰编译产物，不淘汰文本。规则/脚本规模增长直接推高堆内存。

## 2. 目标

- 规则和脚本**真正以持久化存储（SQL DB / Redis）为权威源**，JVM 只是有界缓存。
- 执行热路径**不因此变慢**：缓存命中时零远程调用。
- JVM 内存占用与规则总量**解耦**：常驻的只有轻量索引，内容进有界缓存。
- 存储中的变更能让**所有节点最终一致地收敛**到新版本（接受秒级窗口），即使通知丢失也必收敛。
- **极致好用**：配置能推断的绝不让用户配，最小配置为零～三行。

### 非目标（v1 不做）

- 旧的 6 个规则插件不动，一行不改；旧模式与新模式不共存（启动互斥检查）。
- zk/nacos/etcd/apollo 的 Rule-DB 实现（SPI 已就位，留作后续）。
- 节点实例 ID 持久化（旧 sql 插件的 `NodeInstanceIdManageSpi` 能力，留作后续扩展）。
- 原子切换（所有节点同一逻辑时刻切换版本）——本设计的一致性语义是**最终收敛，秒级窗口**。
- 管理界面/控制台。仅提供发布 API 与写入规范。

## 3. 已确认的关键决策

| 决策点 | 结论 |
|---|---|
| 存储结构 | 全新表结构 / 键结构，不兼容旧插件 |
| 一致性语义 | 最终收敛，秒级窗口；通知失效 + 周期对账兜底 |
| 内存策略 | 只常驻「id → 版本戳 + 轻量元数据」索引；EL 文本、脚本源码、编译产物全部进有界缓存 |
| 写入路径 | 框架提供发布 API（原子完成内容+版本+序号+通知）+ 文档化 SQL/Lua 写入规范；content_md5 对账双保险 |
| SQL 变更感知 | 轻量轮询变更序号表（`SELECT MAX(seq)`，走索引），不强制引入通知中间件 |
| 模块落点 | 两个全新模块 `liteflow-rule-db-sql`、`liteflow-rule-db-redis`，旧插件零改动 |
| 失效策略 | 分级刷新：驻留条目后台异步刷新（旧版继续服务），影子条目只更新索引 |
| 脚本淘汰 | 引用计数联动 chain 淘汰，不设独立容量 |
| 配置形态 | 全新一等公民命名空间 `liteflow.rule-db.*`（带 configuration metadata），不使用 ext-data JSON |

## 4. 总体架构

```
┌─────────────── 应用节点 (每台 JVM) ───────────────┐
│                                                    │
│  FlowExecutor ── 执行热路径（命中缓存＝零远程调用）      │
│       │                                            │
│  FlowBus（新模式下的形态）                            │
│   ├── RuleIndex   常驻：id → 版本戳 + 轻量元数据       │
│   └── RuleCache   有界：编译后的 Chain / 脚本产物       │
│       │ 未命中/已失效 → 按需拉取 + 编译                 │
│  RuleRepository SPI ←─ ChangeWatcher（订阅/轮询序号）  │
│       │                Reconciler（低频清单对账）      │
└───────┼────────────────────────────────────────────┘
        │
   ┌────┴─────┐
   │ 权威存储  │  Redis 或 SQL DB（内容 + 版本 + 变更序号 + 通知）
   └────┬─────┘
        │
  RulePublisher（发布 API：原子完成 内容+版本+序号+通知）
```

数据面与控制面分离：执行（数据面）只接触本地缓存；版本同步（控制面）由 ChangeWatcher/Reconciler 异步驱动。

## 5. core 新增：`RuleRepository` SPI 与运行时设施

新包 `com.yomahub.liteflow.repository`，通过 ServiceLoader 装载（仿 `ContextAwareHolder` 模式，提供 `RuleRepositoryHolder`）。

```java
public interface RuleRepository {
    RuleManifest fetchManifest();                    // 清单：全部 chain/script 的 id + version + md5 + 脚本元数据（不含内容）
    ChainRecord fetchChain(String chainId);          // {el, route, namespace, version, md5, enable}
    ScriptRecord fetchScript(String nodeId);         // {script, name, type, language, version, md5, enable}
    long fetchLatestSeq();                           // 当前最大变更序号
    List<ChangeRecord> fetchChangesSince(long seq);  // 增量变更；seq 已断档时抛 SeqGapException
    default void subscribe(RuleChangeListener l) {}  // 可选推送通道（Redis 实现，SQL 不实现）
}
```

- `ChangeRecord = {seq, targetType(CHAIN/SCRIPT), targetId, op(UPSERT/DELETE), version}`。
- `RuleManifest` 中脚本条目必须带 `type/language/name` 元数据：EL 编译时 `NodeOperator` 需要知道节点存在与类型，但不需要脚本内容。

core 侧其余设施：

- **RuleIndex**：常驻内存。`chainId → {version, md5}`；`nodeId → {version, md5, type, language, name}`。
- **RuleCache**：有界（容量 = `cache-capacity`，按 chain 条数计）。管理编译后 Chain 的驻留与淘汰、脚本产物的引用计数。基于现有 `ChainCacheLifeCycle` 思路扩展，但淘汰语义增强（见 §8）。
- **ChangeWatcher**：Redis 下订阅 notify channel + 低频 seq 校验兜底；SQL 下按 `seq-poll-seconds` 轮询 `MAX(seq)`。
- **Reconciler**：按 `reconcile-seconds` 全量清单对账。
- **影子状态**：FlowBus 中的 Chain 允许"只有 chainId、无 EL、未编译"的影子形态；脚本 Node 允许"有元数据、无源码、未编译"的影子形态。

## 6. 存储结构

### 6.1 SQL（三张表，表名前缀可配，默认 `lf_`；字段名固定；DDL 随模块提供）

**`lf_chain`** — PK(`application_name`, `chain_id`)

| 列 | 类型 | 说明 |
|---|---|---|
| application_name | VARCHAR(64) | 应用隔离维度 |
| chain_id | VARCHAR(128) | |
| namespace | VARCHAR(64) NULL | |
| el_data | TEXT | EL 表达式 |
| route_data | TEXT NULL | 路由 EL（route chain） |
| version | BIGINT | 每次发布 +1 |
| content_md5 | CHAR(32) | el_data + route_data 的 MD5 |
| enable | TINYINT | 1 启用 / 0 停用 |
| gmt_create / gmt_modified | DATETIME | |

**`lf_script`** — PK(`application_name`, `node_id`)

| 列 | 类型 | 说明 |
|---|---|---|
| application_name | VARCHAR(64) | |
| node_id | VARCHAR(128) | |
| script_name | VARCHAR(128) NULL | |
| script_type | VARCHAR(32) | 对齐 `NodeTypeEnum` 脚本类型（script / boolean_script / switch_script / for_script / while_script / break_script / iterator_script） |
| script_language | VARCHAR(32) NULL | groovy / js / python … 为空用全局默认 |
| script_data | TEXT | 脚本源码 |
| version / content_md5 / enable / gmt_create / gmt_modified | 同上 | |

**`lf_change_log`** — PK `seq` AUTO_INCREMENT，索引 (`application_name`, `seq`)

| 列 | 说明 |
|---|---|
| seq | 全局单调递增变更序号 |
| application_name | |
| target_type | CHAIN / SCRIPT |
| target_id | chainId / nodeId |
| op | UPSERT / DELETE |
| version | 变更后的版本号 |
| gmt_create | |

change_log 允许运维定期清理（建议保留 7 天）；节点发现自己的 lastAppliedSeq 已小于表中最小 seq（断档）时自动触发全量对账，清理不影响正确性。

### 6.2 Redis（键前缀可配，默认 `lf`；下述 `{app}` 为 application-name）

```
lf:{app}:chain:{chainId}    HASH   el / route / namespace / version / md5 / enable
lf:{app}:script:{nodeId}    HASH   script / name / type / language / version / md5 / enable
lf:{app}:chain-index        HASH   chainId → "version|md5"
lf:{app}:script-index       HASH   nodeId  → "version|md5|type|language|name"
lf:{app}:seq                STRING （INCR）
lf:{app}:changelog          ZSET   score=seq，member = JSON{seq, targetType, targetId, op, version}
lf:{app}:notify             pub/sub channel，消息 = JSON{seq, targetType, targetId, op, version}
```

index 两个 HASH 使清单/对账一次 `HGETALL` 完成，不必扫描内容键。

## 7. 发布协议（RulePublisher）

各插件模块内提供，**可独立使用**：管理后台只依赖插件 jar 即可调用，不需要拉起 FlowExecutor。

```java
public interface RulePublisher {
    long publishChain(String chainId, String el);            // 返回新版本号；重载支持 route/namespace
    long publishScript(ScriptRecord script);
    void removeChain(String chainId);
    void removeScript(String nodeId);
    void enableChain(String chainId, boolean enable);        // script 同理
}
```

- **SQL 实现**：单事务内 UPSERT 内容行（`version = version + 1`，重算 md5）+ INSERT change_log。
- **Redis 实现**：一段 Lua 原子完成 HSET 内容 → HSET index → INCR seq → ZADD changelog（score=seq）→ PUBLISH 通知，五步在单脚本内原子提交；changelog ZSet 的 member 与 notify 消息同构（JSON{seq, targetType, targetId, op, version}），既支撑 seq 轮询的 `fetchChangesSince` 区间拉取，也使断档检测（最小 score &gt; since+1）成为可能。
- 同时文档化等价的 SQL 写法 / Lua 脚本规范，供已有管理后台不依赖 Java 客户端也能按规范写入。
- **双保险**：对账时 version 相同再比 content_md5，能发现"绕过规范改了内容但没动版本号"的脏写。

## 8. 运行时机制

### 8.1 启动

1. classpath 存在 rule-db 插件且 `enabled=true` → FlowExecutor init 走新路径，**不做全量拉取与 XML 解析**；检测到同时配置了 `rule-source` → 启动报错（互斥）。
2. `fetchManifest()` → 构建 RuleIndex；向 FlowBus 注册影子 Chain 与影子脚本 Node。
3. 记录 `lastAppliedSeq = fetchLatestSeq()`。
4. 启动 ChangeWatcher 与 Reconciler。
5. `preload-chain-ids` 配置的 chain 立即拉取编译（抹平关键链路的冷启动尖刺）。

### 8.2 执行热路径

```
execute2Resp(chainId)
  → FlowBus.getChain(chainId)                      // 本地 map
  → 已编译且版本戳与索引一致 → 直接执行                 // 零远程调用，唯一新增开销 = 一次 volatile 版本比对
  → 否则（影子/已失效）→ Chain 上 double-checked locking：
       repository.fetchChain(chainId)              // 一次远程读，带重试
       → LiteFlowChainELBuilder 构建条件树
       → 写入 RuleCache，登记引用的脚本节点（引用计数 +1）
  → 子链引用（chain 调 chain）递归同一懒加载路径
  → 执行到脚本节点且执行器无产物：
       per-node double-check → fetchScript → loadScript → 缓存产物
```

### 8.3 有界缓存与淘汰

- 容量按 chain 条数配置（`cache-capacity`，默认 500），LRU 语义（沿用现有 chain 缓存的保活思路）。
- **chain 淘汰** = 丢弃条件树 + EL 文本 → 退回影子状态；其引用的脚本引用计数 −1。
- **脚本淘汰** = 引用计数归零时 `executor.unLoad(nodeId)` + 清脚本文本 → 退回影子 Node。计数在编译时登记（遍历条件树收集脚本节点）、在缓存管理器同步域内增减，不触热路径。
- 不为脚本设独立容量：避免"chain 驻留但脚本被淘汰"的热路径抖动和双参数难配问题。

### 8.4 失效与收敛（分级刷新）

ChangeWatcher 拿到 `fetchChangesSince(lastAppliedSeq)` 后逐条处理，处理完推进 lastAppliedSeq：

| 变更 | 驻留缓存中 | 影子状态 |
|---|---|---|
| UPSERT chain | 更新索引 + **后台异步刷新**（拉新版编译，原子替换；期间旧版继续服务） | 只更新索引版本戳 |
| UPSERT script | 更新索引 + 后台刷新脚本产物（同 nodeId 替换，条件树不重建）；若 type 变化则把引用它的 chain 一并失效 | 只更新索引 |
| DELETE | 移除索引 + 缓存，后续执行报 chain 不存在 | 移除索引 |

- 后台刷新走**单线程串行队列**：批量发布 × 多节点也不会集中回源（每节点只刷新自己驻留的活跃集）。
- 刷新失败：旧版继续服务 + 标记 stale，由下次执行或对账重试——变更推送路径的故障不影响线上执行。
- **Reconciler**（默认 60s）：拉清单全量 diff（先比 version，相同再比 md5），修正索引与缓存、快进 lastAppliedSeq。覆盖 pub/sub 丢消息、change_log 被清理、订阅断线窗口。
- **收敛保证**：任何变更最迟在 `max(通知延迟, seq 轮询周期, 对账周期)` 内被所有节点感知；版本号单调递增，不会新旧回跳。

> 实现注记（2026-07-10）：v1 的"分级刷新"以惰性失效落地——驻留条目收到变更后失效缓存态、下次执行懒加载新版，配合执行中持有旧 conditionList 引用跑完的既有语义达成不中断切换。"后台预编译零首个请求延迟"作为后续增强。

### 8.5 并发与竞态

- 同一 chain 冷启动并发：Chain 对象上 double-checked locking，仅一个线程回源。
- fetch 与发布竞态：以 fetch 回内容行自带的 version 为准入缓存；通知到达时缓存版本 ≥ 通知版本则忽略（幂等）。
- 执行中变更：进行中的执行持有旧条件树引用跑完，新执行拿新版（与现有 copy-on-write 语义一致）。
- 订阅断线重连成功后强制触发一次对账，堵住断线窗口。

## 9. 错误处理与降级

| 故障场景 | 行为 |
|---|---|
| 存储不可用，缓存命中 | 照常执行，完全不受影响（核心可用性属性） |
| 存储不可用，缓存未命中 | fetch 按 `fetch-retry-times` 重试，仍失败抛新异常 `ChainLoadException`（语义区别于 `ChainNotFoundException`） |
| 订阅断线 | 自动重连 + 重连后强制对账 |
| change_log 清理导致 seq 断档 | `fetchChangesSince` 抛 `SeqGapException` → 自动全量对账 |
| fetch 到 enable=false 或行不存在 | 从索引移除，后续执行报 chain 不存在 |
| 后台刷新失败 | 旧版继续服务 + 标记 stale，对账周期重试 |
| 缺表且未开 auto-init-table | 启动报错，错误信息内含完整可复制执行的 DDL |

## 10. 配置设计（`liteflow.rule-db.*`）

配置哲学：能推断的绝不让用户配；必须配的压到最少；一个命名空间集中所有参数；Boot/Solon starter 提供 configuration metadata（IDE 自动补全）。非 Spring 环境通过 `LiteflowConfig` 编程式设置同一配置对象（core 新增 `RuleDbConfig` 属性类，starter 负责绑定）。

**最小配置示例：**

```properties
# ① Spring Boot 应用已有 DataSource：引入 liteflow-rule-db-sql 依赖 → 零配置
#    自动复用容器 DataSource，application-name 自动取 spring.application.name

# ② 规则放独立数据库：三行（驱动类从 url 自动推断）
liteflow.rule-db.url=jdbc:mysql://host:3306/rules
liteflow.rule-db.username=root
liteflow.rule-db.password=xxx

# ③ Redis：一行起步（容器有 RedissonClient bean 时同样零配置）
liteflow.rule-db.address=redis://127.0.0.1:6379
#    哨兵 = 多地址 + master-name；集群 = 多地址不配 master-name；部署模式自动推断
```

**完整配置面（全部有默认值）：**

| 配置项 | 默认 | 说明 |
|---|---|---|
| `enabled` | true | 引入依赖即激活；逃生开关 |
| `application-name` | spring.application.name | 多应用共库隔离维度；非 Spring 必填 |
| `cache-capacity` | 500 | 有界缓存容量（chain 条数） |
| `seq-poll-seconds` | SQL:3 / Redis:30 | 变更序号轮询间隔（Redis 有 pub/sub，仅兜底） |
| `reconcile-seconds` | 60 | 清单对账周期 |
| `preload-chain-ids` | 空 | 启动预热清单 |
| `fetch-retry-times` | 3 | 回源重试次数 |
| SQL `url` / `username` / `password` | — | 不配 url 则自动复用容器 DataSource |
| SQL `driver-class-name` | 从 url 推断 | |
| SQL `datasource-bean-name` | 自动查找 | 多 DataSource 时指定 |
| SQL `table-prefix` | `lf_` | 字段名固定不可配 |
| SQL `auto-init-table` | false | 开启则启动 `CREATE TABLE IF NOT EXISTS` |
| Redis `address` | — | 单机/哨兵/集群统一入口，多地址逗号分隔 |
| Redis `master-name` | — | 配置即哨兵模式 |
| Redis `username` / `password` / `database` | — / — / 0 | |
| Redis `key-prefix` | `lf` | |
| Redis `redisson-bean-name` | 自动查找 | 复用容器 RedissonClient |

**与旧配置的关系**：新模式下 `parseMode`、`enableMonitorFile`、`chainCacheEnabled/Capacity`、`rule-source` 均不适用；检测到配置了 `rule-source` 直接启动报错（互斥），其余项忽略并打 warn 说明语义由 `rule-db.*` 接管。

## 11. 模块落点与兼容性

```
liteflow-rule-plugin/
  liteflow-rule-db-sql/      SqlRuleRepository + SqlRulePublisher + DDL（依赖 JDBC，驱动由用户提供）
  liteflow-rule-db-redis/    RedisRuleRepository + RedisRulePublisher（依赖 Redisson，Lua 原子发布）
liteflow-core/
  com.yomahub.liteflow.repository/   RuleRepository SPI、RuleIndex、RuleCache、ChangeWatcher、Reconciler、RuleDbConfig
```

- 两个新模块通过 ServiceLoader 注册 `RuleRepository`，**不实现** `ParserClassNameSpi`（不是解析器，不走拼 XML 路径）。两个插件同时在 classpath → 启动报错要求二选一。
- 旧 6 个插件、三种 parseMode、全量 XML 路径零改动，新模式纯增量。
- 普通 Java 组件 / 声明式组件不受影响（本方案只纳管 EL 与脚本）。
- `LiteFlowChainELBuilder` 手动 build 的 chain 与本模式共存：以手动 build 的为准并打 warn；Reconciler 只管理来源于清单的条目，不会把手动 chain 当作"存储中不存在"而删除。
- 模块纳入根 pom 两个 compile profile（JDK 8 语法基线，与其他 rule 插件一致），发布走 `release-on-8`。

## 12. 测试策略

遵守仓库强制约定：所有测试放 `liteflow-testcase-el/` 下，新建两个测试模块：

```
liteflow-testcase-el/liteflow-testcase-el-rule-db-sql      （H2）
liteflow-testcase-el/liteflow-testcase-el-rule-db-redis    （沿用现有 redis 测试基建）
```

覆盖场景：

1. **懒加载**：启动只建索引 → 首次执行拉取编译 → 二次执行零回源（用计数桩断言回源次数）。
2. **变更收敛**：`RulePublisher` 发布新版 → ≤ 轮询/通知周期内 → 断言执行结果为新逻辑。
3. **淘汰与重载**：容量设小 → 触发淘汰 → 断言退影子 + 脚本 unload（引用计数归零）→ 再执行能重载。
4. **对账兜底**：绕过通知直接改存储（模拟丢消息）→ 对账周期内收敛；version 不动只改内容 → md5 双保险生效。
5. **降级**：断开存储 → 缓存命中的 chain 仍可执行；未命中报 `ChainLoadException`。
6. **多节点一致性**：FlowBus 是静态单例，单 JVM 起不了两个节点；用"另一节点发布"（直接 publisher 写存储）+ 本节点收敛来等价验证协议正确性。
7. **配置推断**：DataSource/RedissonClient bean 复用、驱动类推断、Redis 部署模式推断、application-name 默认值。
8. **互斥与报错**：与 `rule-source` 共存报错、双插件共存报错、缺表报错含 DDL。

## 13. 依赖与发布

- `liteflow-rule-db-sql`：仅依赖 JDBC 标准 API（驱动用户自带），H2 为测试依赖。
- `liteflow-rule-db-redis`：依赖 Redisson（与旧 redis 插件同选型，运维认知一致）。
- 两模块均为 JDK 8 语法基线；starter 侧的 `liteflow.rule-db.*` 绑定与 configuration metadata 分别落在 `liteflow-spring-boot-starter`、`liteflow-spring-boot4-starter`、`liteflow-solon-plugin`。
