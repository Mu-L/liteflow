# Rule-DB 四后端统一架构与 Publisher 设计

日期：2026-07-12

## 1. 背景

当前 Rule-DB 已有 SQL 和 Redis 两个实现。核心运行时采用“权威存储中的元数据索引常驻，规则正文和编译产物按需加载并有界缓存”的模型，但两个实现的变更感知方式不同：SQL 使用变更序号轮询，Redis 同时使用 pub/sub、变更序号轮询和周期对账。Publisher 也分别由 `SqlRulePublisher` 和 `RedisRulePublisher` 暴露，调用方必须依赖后端专用 API。

仓库中还存在传统的 `liteflow-rule-etcd` 和 `liteflow-rule-zk` 插件。它们在启动时将全部规则拼装并加载进 JVM，后续再通过 watch 热更新，仍然属于全量 Rule Source 模式，不具备 Rule-DB 的懒加载和有界缓存特性。

本设计完成以下收敛：

1. SQL 和 Redis 统一采用“变更日志轮询＋周期对账”，Redis 删除 pub/sub。
2. 新增 `liteflow-rule-db-etcd` 和 `liteflow-rule-db-zk`，采用“原生 watch 订阅＋周期对账”。
3. 四个后端共用同一套 Rule-DB 运行时、缓存、懒加载、版本和错误恢复机制，仅替换存储适配与变更感知实现。
4. 新增独立的 `liteflow-rule-db-publisher` 模块，对外提供统一发布 API。
5. 允许绕过 Publisher 进行规范人工修改。人工修改正文时必须同时递增业务 `version`，可以不写变更日志，最终由 manifest 对账发现。

## 2. 目标与非目标

### 2.1 目标

- 四个后端以存储为唯一权威源，JVM 只常驻轻量元数据索引和有界执行缓存。
- 启动时不加载全部 chain EL 和 script 源码。
- SQL 与 Redis 在机制和默认收敛语义上保持一致。
- etcd 与 ZK 使用各自可靠的原生 watch 作为低延迟通道。
- 所有后端都保留周期 manifest 对账，覆盖人工修改、通知丢失、断线和日志断档。
- Publisher 的业务操作 API 与后端无关，连接和原子写入细节由 DB 模块实现。
- 新版本加载失败时保留最后成功版本，错误发布不立即击穿已有线上流程。
- 通过共享契约测试验证四个后端的一致行为。

### 2.2 非目标

- 不删除或改造传统的 `liteflow-rule-etcd`、`liteflow-rule-zk` 插件。
- 不让传统 Rule Source 插件与新 Rule-DB 模块共用运行时。
- 不兼容当前未对外使用的 `SqlRulePublisher` 和 `RedisRulePublisher` 公共类。
- 不保证“只改正文但不递增 version”的裸改能够生效。
- 不在第一版实现跨后端复制、双写、分布式事务或统一管理控制台。
- 不让 Publisher 依赖执行端的 `LiteflowConfigGetter` 全局配置。

## 3. 方案比较

### 3.1 各后端独立运行时

每个后端自行实现加载、监听、缓存和刷新。实现初期直接，但四套状态机必然逐渐分叉，人工修改和错误恢复语义也难以保持一致。本方案不采用。

### 3.2 强制所有后端使用同一种通知技术

可以为四个后端都模拟 pub/sub，或者都只做 manifest 轮询。前者让 SQL/Redis 引入额外基础设施，后者放弃 etcd/ZK 的原生低延迟能力。本方案不采用。

### 3.3 统一核心协议，后端声明变更感知能力

核心统一负责元数据索引、懒加载、有界缓存、版本状态、事件幂等、对账和最后成功版本；后端只实现 `RuleRepository`、`RuleChangeSource` 和 Publisher Provider。

SQL/Redis 的 ChangeSource 使用轮询，etcd/ZK 的 ChangeSource 使用订阅。四者对核心都表现为统一 `ChangeRecord` 流，并共享同一个 manifest 对账通道。本方案既保留后端优势，又不会复制运行时逻辑，因此作为最终方案。

## 4. 模块与依赖

最终模块布局：

```text
liteflow-rule-db/
  pom.xml
  liteflow-rule-db-publisher/
  liteflow-rule-db-sql/
  liteflow-rule-db-redis/
  liteflow-rule-db-etcd/
  liteflow-rule-db-zk/
```

职责划分：

| 模块 | 职责 |
|---|---|
| `liteflow-core` | Rule-DB SPI、元数据与变更 VO、同步状态机、懒加载、有界缓存、对账和生命周期 |
| `liteflow-rule-db-publisher` | 统一 Publisher API、公共请求与结果、Provider SPI、Factory、统一异常 |
| `liteflow-rule-db-sql` | SQL Repository、轮询 ChangeSource、Publisher Provider、DDL 和连接管理 |
| `liteflow-rule-db-redis` | Redis Repository、轮询 ChangeSource、Publisher Provider、Lua 和连接管理 |
| `liteflow-rule-db-etcd` | etcd Repository、watch ChangeSource、Publisher Provider 和连接管理 |
| `liteflow-rule-db-zk` | ZK Repository、watch ChangeSource、Publisher Provider 和连接管理 |

依赖方向：

```text
liteflow-rule-db-publisher -> liteflow-core
liteflow-rule-db-{sql,redis,etcd,zk} -> liteflow-core + liteflow-rule-db-publisher
```

执行应用只依赖一个具体 DB 模块。后台管理应用依赖 `liteflow-rule-db-publisher` 和一个对应 DB 模块，例如：

```xml
<dependency>
    <groupId>com.yomahub</groupId>
    <artifactId>liteflow-rule-db-publisher</artifactId>
</dependency>
<dependency>
    <groupId>com.yomahub</groupId>
    <artifactId>liteflow-rule-db-sql</artifactId>
</dependency>
```

每个 DB 模块注册两个 SPI：

```text
com.yomahub.liteflow.repository.RuleDbProvider
com.yomahub.liteflow.publisher.RulePublisherProvider
```

执行端只允许发现一个 `RuleDbProvider`。发现多个实现时启动失败，并在异常中列出实现类和可能冲突的模块。Publisher Factory 按配置类型选择 `RulePublisherProvider`，未找到或找到多个匹配实现时立即失败。

## 5. 核心抽象

### 5.1 RuleDbProvider

`RuleDbProvider` 是执行端加载的顶层 SPI：

```java
public interface RuleDbProvider extends AutoCloseable {

    RuleRepository repository();

    RuleChangeSource changeSource();

    @Override
    void close();
}
```

Provider 负责共享和关闭后端连接资源。Repository 和 ChangeSource 不分别创建无法协调的客户端。

### 5.2 RuleRepository

Repository 只负责读取权威数据，不再包含 `subscribe`、默认轮询周期或 ChangeSource 生命周期：

```java
public interface RuleRepository {

    RuleManifest fetchManifest();

    ChainMeta fetchChainMeta(String chainId);

    ScriptMeta fetchScriptMeta(String nodeId);

    ChainRecord fetchChain(String chainId);

    ScriptRecord fetchScript(String nodeId);
}
```

`fetchManifest()` 只返回所有启用对象的元数据和本次快照位置，不返回 chain EL 或 script 源码。`fetchChainMeta()` 和 `fetchScriptMeta()` 用于新增对象事件，避免为了注册一个影子对象重新拉取完整 manifest 或提前加载正文。

### 5.3 RuleChangeSource

ChangeSource 向核心暴露统一事件流，并采用两阶段启动消除初始化窗口：

```java
public interface RuleChangeSource extends AutoCloseable {

    void open(RuleChangeListener listener);

    void activate(long baselineSeq);

    ChangeSourceHealth health();

    @Override
    void close();
}
```

- `open`：建立底层资源并进入缓冲状态。SQL/Redis 暂不轮询；etcd/ZK 建立 watch 并缓冲事件。
- `activate`：manifest 初始化完成后设置基线，丢弃 `seq <= baselineSeq` 的事件，按序回放更大的事件，然后进入实时状态。
- `health`：返回 `STARTING`、`UP`、`DEGRADED` 或 `DOWN` 及最近错误、最近成功时间和游标。
- `close`：幂等停止调度、watch 和回调。

SQL/Redis ChangeSource 内部调用后端变更日志读取能力。该能力不再暴露在 `RuleRepository` 中，可作为模块内部接口实现：

```java
interface PollingChangeLog {

    long fetchLatestSeq();

    List<ChangeRecord> fetchChangesSince(long seq);
}
```

### 5.4 统一变更模型

```text
ChangeRecord
  seq
  targetType: CHAIN | SCRIPT
  targetId
  op: UPSERT | DELETE
  version
```

`seq` 是后端事件位置，`version` 是业务内容版本，两者不能混用：

| 后端 | seq |
|---|---|
| SQL | `change_log.seq` |
| Redis | 全局 `INCR` 序号 |
| etcd | etcd revision |
| ZK | metadata znode 的 `mzxid` |

业务 `version` 必须单调递增。Publisher 保证原子递增；人工规范修改负责显式递增。核心以 `version` 判断目标内容新旧，以 `seq` 排序、去重和检测轮询日志断档。

## 6. 元数据、正文与存储协议

### 6.1 逻辑模型

每个规则对象在逻辑上分为 metadata 与 content：

```text
RuleMetadata
  targetId
  version
  contentMd5
  enable
  target-specific metadata

RuleContent
  chain: el, route, namespace
  script: source, name, type, language
```

物理存储可以同表、同 Hash 或不同 key，但 `fetchManifest` 必须能够只读取 metadata。Publisher 必须在后端提供的原子能力范围内同时提交 metadata、content 和可观察的变更位置或日志。

### 6.2 SQL

SQL 沿用 chain、script 和 change log 三张表。manifest 查询只选择 ID、version、md5 及脚本类型等元数据列，不选择正文列。

Publisher 在同一数据库事务中完成：

```text
校验 expectedVersion
更新或插入正文与元数据
业务 version + 1
插入 change_log
提交
```

删除在同一事务中删除或停用内容行，并插入 DELETE change log。是否采用物理删除或 `enable = 0` 必须在实现中统一；推荐使用 `enable = 0` 保留版本和审计信息，manifest 将其视为不存在。

### 6.3 Redis

Redis 不再使用 `RTopic` 或 `PUBLISH`。推荐键模型：

```text
{prefix}:{app}:chain-ids            SET
{prefix}:{app}:script-ids           SET
{prefix}:{app}:chain:{id}           HASH
{prefix}:{app}:script:{id}          HASH
{prefix}:{app}:seq                  STRING
{prefix}:{app}:changelog            ZSET
```

ID SET 只保存 ID，不重复保存 version。manifest 使用 pipeline 对各 Hash 执行 `HMGET`，只取 version、md5、enable 和脚本元数据字段，不读取正文。这样人工修改已有记录时只需更新一个 Hash 中的正文和 version，不会出现正文 version 与 index version 分叉。

Publisher 使用 Lua 原子完成：

```text
校验 expectedVersion
HSET 内容和元数据
SADD ID
INCR seq
ZADD changelog
```

Lua 中删除 `PUBLISH`。删除时原子执行 `DEL`、`SREM`、`INCR` 和 `ZADD`。变更日志按容量或时间保留，断档由 ChangeSource 抛出 `SeqGapException` 并触发 manifest 对账。

### 6.4 etcd

为避免 prefix range 在启动时返回全部正文，metadata 与 content 使用不同 key：

```text
{root}/{app}/chains/meta/{chainId}
{root}/{app}/chains/content/{chainId}
{root}/{app}/scripts/meta/{nodeId}
{root}/{app}/scripts/content/{nodeId}
```

manifest 只 range `meta` 前缀。按需加载时读取对应 content key，并再次校验 metadata version，避免读取到跨事务或人工错误写入造成的不一致数据。

ChangeSource 只 watch 两个 `meta` 前缀。Publisher 使用 etcd transaction 原子更新 content 和 metadata，metadata 更新必须放在同一事务中并产生可观察的新 revision。`expectedVersion` 通过 compare 条件实现。

发生 watch revision compacted 时，ChangeSource 进入 `DEGRADED`，触发一次 manifest 对账，以新快照 revision 重新建立 watch。

### 6.5 ZK

ZK 同样分离 metadata 与 content：

```text
{root}/{app}/chains/meta/{chainId}
{root}/{app}/chains/content/{chainId}
{root}/{app}/scripts/meta/{nodeId}
{root}/{app}/scripts/content/{nodeId}
```

manifest 只读取 `meta` 子树。ChangeSource 使用 CuratorCache 监听两个 `meta` 子树，并用 metadata znode 的 `mzxid` 作为事件 seq。

Publisher 使用 Curator transaction/multi 原子更新 content 和 metadata。应用业务 version 保存在 metadata 值中；ZK `Stat.version` 只用于 CAS 和并发控制，不替代业务 version。

ZK manifest 不是跨节点原子快照，因此必须先打开并缓冲 CuratorCache，再拉 manifest，最后按最大已观察 `mzxid` 激活事件流。激活后必须立即再执行一次 manifest 对账，覆盖 manifest 读取期间的并发修改窗口。session 失效后重建 cache，并立即执行一次 manifest 对账。

## 7. 启动、同步与对账

统一启动顺序：

```text
1. ServiceLoader 解析唯一 RuleDbProvider
2. RuleChangeSource.open(listener)，开始缓冲事件
3. RuleRepository.fetchManifest()
4. 只注册 chain/script 影子和 desiredVersion 索引
5. 初始化有界缓存，不加载正文
6. RuleChangeSource.activate(manifest.latestSeq)
7. 回放缓冲事件
8. 启动周期 manifest 对账
9. 按配置执行 preload
```

事件应用规则：

- `seq <= lastAppliedSeq` 的重复事件可以忽略，但 manifest 合成事件的 seq 为 0，不参与全局游标推进。
- UPSERT 的 `version < desiredVersion` 时忽略，不能让状态回退。
- UPSERT 的 `version >= desiredVersion` 时更新目标版本并标记缓存过期；等于当前版本的重复事件保持幂等。
- DELETE 以权威删除状态为准，不因旧缓存版本更高而忽略。
- 新增对象先调用 `fetchChainMeta` 或 `fetchScriptMeta` 注册影子，不加载正文。

四个后端都周期执行 manifest 对账。对账比较：

1. ID 是否新增或消失。
2. version 是否变化。
3. 当双方 md5 都存在时，md5 是否变化。

对账完成后仅单调推进游标。manifest 获取失败时保留现有索引和可执行版本，不执行删除推断。

## 8. 最后成功版本

### 8.1 状态模型

每个 chain/script 同时维护：

```text
desiredVersion：权威源要求收敛到的版本
activeVersion：当前成功加载且可以执行的版本
desiredMd5
activeMd5
state：SHADOW | READY | STALE | LOADING | FAILED | DELETED
```

状态语义：

| 状态 | 含义 |
|---|---|
| `SHADOW` | 只有元数据，尚无可执行内容 |
| `READY` | active 与 desired 一致 |
| `STALE` | 已发现新版本，尚未刷新 |
| `LOADING` | 正在拉取和编译候选版本 |
| `FAILED` | 新版本刷新失败，旧 active 仍可执行 |
| `DELETED` | 已删除或禁用，旧版本不可执行 |

### 8.2 刷新规则

收到 UPSERT 时只更新 desired 状态，不先销毁 active 内容。下一次执行由单飞控制器为目标创建候选版本：

```text
读取 metadata
读取 content
再次核对 version
在隔离候选对象中解析或编译
成功后原子替换 active
失败则保留原 active，并记录 desired 失败状态
```

同一目标只允许一个加载任务。已有 active 时，其他并发请求继续使用旧版本；没有 active 的新对象必须等待首次加载并在失败时返回执行错误。

chain 的候选 EL 必须在临时 Chain 上完成条件树构建后再交换。script 必须使用版本化内部编译产物或等价的原子引用，保证候选编译失败不会先卸载正式脚本。script 激活成功后，将引用它的已缓存 chain 标记为 STALE，使这些 chain 在后续执行中使用新脚本重新生成候选条件树。

DELETE 或 `enable = false` 表示明确停止服务，立即进入 `DELETED`，不允许继续执行最后成功版本。

### 8.3 可观测性

至少暴露以下状态和指标：

- Rule-DB Provider 类型和 ChangeSource health。
- 当前同步 seq、最近轮询或 watch 成功时间。
- 最近 manifest 对账时间与结果。
- READY、STALE、LOADING、FAILED、SHADOW 对象数。
- desiredVersion 与 activeVersion 不一致的对象及最近错误。
- 拉取、编译、Publisher 和版本冲突计数。

日志必须包含 application、target type、target ID、desired version、active version 和后端操作，禁止输出密码、token 或完整规则正文。

## 9. Publisher API

### 9.1 公共接口

```java
public interface RulePublisher extends AutoCloseable {

    PublishResult publishChain(PublishChainRequest request);

    PublishResult publishScript(PublishScriptRequest request);

    PublishResult removeChain(RemoveRuleRequest request);

    PublishResult removeScript(RemoveRuleRequest request);
}
```

请求公共字段：

```text
targetId
expectedVersion
```

chain 请求额外包含 EL、route 和 namespace；script 请求额外包含源码、name、type 和 language。`applicationName` 固定在 Publisher 配置中，不允许单次请求切换目标应用。

`expectedVersion` 语义：

- `null`：无条件发布，后端原子生成下一版本。
- `0`：仅当目标不存在时创建。
- 大于 `0`：仅当当前业务版本完全相同时更新或删除。
- 不匹配：抛出 `VersionConflictException`，不得产生部分写入或变更事件。

`PublishResult` 返回 target ID、target type、operation、新业务 version 和后端 sequence/revision。

### 9.2 配置与 Factory

Publisher 不读取 `LiteflowConfigGetter`。公共模块定义配置标记接口，具体 DB 模块提供类型安全的配置类：

```java
public interface RulePublisherConfig {

    String applicationName();

    PublisherBackend backend();
}
```

示例：

```java
RulePublisher publisher = RulePublisherFactory.create(
        SqlPublisherConfig.builder()
                .applicationName("order-app")
                .url("jdbc:mysql://127.0.0.1/liteflow")
                .username("user")
                .password("secret")
                .build());
```

发布操作 API 完全统一，连接配置保持后端类型安全，避免把 SQL、Redis、etcd 和 ZK 字段再次堆进一个松散属性集合。同一后台进程可以创建多个 Publisher 实例，管理多个 application 或环境。

### 9.3 后端原子性

| 后端 | 原子发布机制 |
|---|---|
| SQL | 数据库事务＋业务 version 条件更新＋change log insert |
| Redis | Lua＋业务 version 校验＋Hash/Set/seq/ZSet 原子修改，不执行 PUBLISH |
| etcd | transaction compare/put/delete，同时修改 content 与 metadata |
| ZK | Curator transaction/multi＋Stat.version CAS，同时修改 content 与 metadata |

Publisher 成功只代表权威存储已经提交，不代表所有执行节点已经激活该版本。调用方通过 `PublishResult` 获得版本和 sequence，节点收敛仍由 ChangeSource 与对账完成。

## 10. 人工规范修改

人工修改正文时至少必须更新 manifest 可见 metadata 中的业务 `version`。可以不写 change log，也不需要模拟发布通知。

| 后端 | 规范人工修改要求 | 最快感知路径 | 兜底路径 |
|---|---|---|---|
| SQL | 修改正文列并递增同行 version | 有 change log 时轮询 | manifest 对账 |
| Redis | 修改目标 Hash 的正文并递增同 Hash version；新增时维护 ID SET | 有 changelog 时轮询 | manifest 对账 |
| etcd | 更新 content key，并更新 meta key 中 version | meta watch | manifest 对账 |
| ZK | 更新 content znode，并更新 meta znode 中 version | meta watch | manifest 对账 |

`contentMd5` 建议同步更新，但 version 已变化时不是感知改动的必要条件。Publisher 始终计算并维护 md5。人工删除必须删除或禁用 metadata；只删除 content 会形成损坏记录，执行时保留旧 active 并报告刷新失败，不会被解释为合法删除。

只修改正文而不递增 version 不在支持范围内。核心可以通过 md5 偶然发现部分此类错误，但不得把这种行为写入兼容承诺。

## 11. 异常与恢复

### 11.1 读取和编译

- metadata 或 content 拉取失败：按 `fetchRetryTimes` 重试，失败后保留 active 并进入 FAILED。
- 读取前后 version 不一致：视为并发写入，不激活候选版本，重新调度加载。
- chain EL 解析或 script 编译失败：保留 active，记录结构化错误和指标。
- 新对象没有 active：向调用方返回明确的加载或编译异常。
- manifest 拉取失败：不得清空任何索引或推断删除。

### 11.2 轮询

- 单轮失败不推进 seq。
- change log 断档立即触发 manifest 对账。
- 单条事件解析失败记录原始 sequence 和目标标识，跳过该事件并触发目标级 metadata 回查；正文不写日志。

### 11.3 订阅

- watch 断开后使用有上限的指数退避重连。
- 重连期间周期对账继续运行。
- etcd revision compacted 时立即对账，并从新 manifest revision 恢复。
- ZK session 失效时重建 CuratorCache，并在激活新事件流前立即对账。
- `close` 后到达的回调必须 no-op，不能在销毁后重新填充缓存。

### 11.4 Publisher

统一异常至少包括：

```text
PublisherConfigurationException
PublisherProviderNotFoundException
VersionConflictException
RuleValidationException
RuleStorageException
```

所有 Publisher 实现必须满足“成功即完整提交，失败即无可见部分提交”。无法满足该条件的人工外部写入不属于 Publisher 保证范围。

## 12. 配置

执行端配置按职责和后端分组：

```yaml
liteflow:
  rule-db:
    enabled: true
    application-name: order-app

    cache:
      capacity: 500
      preload-chain-ids: chain1,chain2

    sync:
      poll-seconds: 3
      reconcile-seconds: 60
      fetch-retry-times: 3

    sql:
      url: jdbc:mysql://127.0.0.1/liteflow
      username: user
      password: secret
      table-prefix: lf_

    redis:
      address: redis://127.0.0.1:6379
      key-prefix: lf

    etcd:
      endpoints: http://127.0.0.1:2379
      root-path: /liteflow

    zk:
      connect-string: 127.0.0.1:2181
      root-path: /liteflow
```

默认行为：

- SQL/Redis：`poll-seconds = 3`。
- etcd/ZK：不创建 seq 轮询任务。
- 四个后端：`reconcile-seconds = 60`、`fetch-retry-times = 3`。
- watch 重连参数第一版使用内部合理默认值，不增加用户配置面。
- `preload-chain-ids` 只预热指定 chain 及其实际引用脚本，不改变全局懒加载模型。

具体实现应继续支持现有容器连接复用能力，例如 SQL DataSource bean 和 RedissonClient bean。etcd/ZK 是否复用容器客户端按各集成框架的既有模式实现。

## 13. 旧插件共存边界

以下传统插件继续保留，行为不变：

```text
liteflow-rule-plugin/liteflow-rule-etcd
liteflow-rule-plugin/liteflow-rule-zk
```

新模块使用不同 artifactId：

```text
liteflow-rule-db-etcd
liteflow-rule-db-zk
```

传统插件属于 `rule-source` 全量加载模式，新模块属于 Rule-DB 权威存储模式。一个应用不能同时启用 `rule-source` 和 Rule-DB，也不能同时引入同一后端的新旧插件。启动校验和文档必须给出明确错误与迁移说明。

## 14. 测试策略

### 14.1 核心单元测试

- 两阶段 ChangeSource 初始化期间的事件缓冲、排序和去重。
- desired/active 状态转换与低版本事件拒绝。
- manifest 对账的新增、更新、删除和 md5 辅助比较。
- 单飞加载与并发线程使用最后成功版本。
- chain 候选构建失败和 script 候选编译失败不会破坏 active。
- destroy 与迟到回调并发时不重新填充状态。
- 缓存淘汰、脚本引用计数和版本化脚本产物释放。

### 14.2 四后端共享契约测试

同一套测试契约由 SQL、Redis、etcd 和 ZK 集成测试分别执行：

1. 启动只获取 metadata，不读取全部正文。
2. 首次执行按需加载 chain 和实际引用脚本。
3. Publisher 发布新增、更新、删除后最终收敛。
4. 人工递增 version 但不写 change log，最终由对账生效。
5. 同一事件重复投递保持幂等。
6. 乱序低版本事件不覆盖高版本。
7. 变更日志断档或 watch 丢失后，对账恢复一致。
8. 新版本正文损坏时继续执行最后成功版本。
9. 删除或禁用后不继续执行最后成功版本。
10. 缓存容量小于规则总量时按需加载与淘汰正确。
11. `expectedVersion` 创建、更新、删除和冲突行为一致。
12. 多执行节点最终收敛到相同 desiredVersion 和 activeVersion。

### 14.3 后端专项测试

- SQL：事务回滚、并发条件更新、H2/MySQL 方言和连接池容量为 1。
- Redis：Lua 原子性、无 `PUBLISH` 调用、ZSet 日志裁剪和 pipeline manifest。
- etcd：transaction CAS、watch revision、compaction 和重连。
- ZK：multi 原子性、Stat.version CAS、session expiration 和 CuratorCache 重建。

### 14.4 构建与文档验证

- 四个 DB 模块及 Publisher 模块独立构建。
- Spring Boot、Spring Boot 4 和 Solon 配置绑定与 metadata 覆盖新增分组。
- Rule-DB 使用指南同时描述 Publisher 路径与人工规范修改路径。
- 旧 etcd/ZK 插件文档明确保留，并与新 artifact 区分。

## 15. 实施顺序

1. 在 core 引入 `RuleDbProvider`、`RuleChangeSource` 和两阶段启动，拆除 `RuleRepository.subscribe`。
2. 将运行时版本状态重构为 desired/active，并实现最后成功版本和候选原子交换。
3. 创建 `liteflow-rule-db-publisher`，定义 API、配置标记、Provider SPI、Factory 和统一异常。
4. 适配 SQL，作为轮询 ChangeSource 和 Publisher 契约基准。
5. 重构 Redis 数据索引与 Lua，删除 pub/sub，并通过与 SQL 相同的轮询契约。
6. 实现 etcd metadata/content 存储、Repository、watch ChangeSource 和 Publisher Provider。
7. 实现 ZK metadata/content 存储、Repository、watch ChangeSource 和 Publisher Provider。
8. 补共享契约测试、后端专项测试、配置 metadata、使用指南和旧插件边界说明。

每一步都应保持已有非 Rule-DB 执行路径可构建、可测试。core 状态机和 Provider SPI 完成后再并行实现各后端，避免四个模块复制过渡接口。

## 16. 验收标准

- Redis 实现中不存在 Rule-DB 规则变更的 `PUBLISH`、`RTopic` 或订阅监听。
- SQL 和 Redis 使用相同的轮询 ChangeSource 契约和默认周期。
- etcd 和 ZK 启动时只加载 metadata，正文读取次数与实际访问规则相关，而不是与规则总数相关。
- 四个后端均使用 core 的同一套状态机、缓存和 manifest 对账代码。
- 后台管理应用通过 `liteflow-rule-db-publisher` 加一个对应 DB 模块即可使用统一 `RulePublisher` API。
- Publisher 与人工规范修改两条路径都能最终使所有执行节点收敛。
- 人工只改正文不递增 version 的行为被明确拒绝为不受支持。
- 新版本加载或编译失败时已有 chain 继续执行 activeVersion；新增、删除和禁用行为符合本设计。
- 旧 `liteflow-rule-etcd` 和 `liteflow-rule-zk` 保持可用，且不会被新 Rule-DB SPI 误加载。
- 核心单元测试、四后端共享契约测试和后端专项集成测试全部通过。
