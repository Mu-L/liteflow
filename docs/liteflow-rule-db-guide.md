# LiteFlow Rule-DB 模式使用指南

LiteFlow 的 Rule-DB 模式让规则和脚本**真正以 SQL 数据库 / PostgreSQL / MongoDB / Redis / ZooKeeper / etcd / Nacos 为权威源**。JVM 常驻规则清单、影子 `Chain` / `Node` 和状态索引，EL／脚本正文及编译产物进入有界缓存。它解决了原有 6 个规则插件「启动拼一份大 XML、规则正文全量常驻堆内存、多节点各跑各的没有一致性保证」的本质痛点：多节点能在秒级窗口内收敛到同一版本，且重内容的常驻规模由缓存容量控制。规则元数据仍随规则总量线性增长，容量边界见 [§10](#10-内存与性能)。

本文分两部分：

- **上手篇**：从「它和老的规则插件有什么不同」讲起，带你用七种后端跑通第一条 Rule-DB 规则。先看这部分。
- **参考篇**：每个配置项、表结构/键结构/路径结构、发布协议、一致性模型、降级语义、可观测性、限制清单，需要查细节时再来。

读完上手篇你应该能：引入一个依赖 → 写三行（或零行）配置 → 用发布 API 发布一条规则 → 像平时一样 `flowExecutor.execute2Resp(...)` 执行它。

> 本能力由根级独立父模块 `liteflow-rule-db` 聚合的七个插件模块提供，随 `2.16.1` 发布：
> - **`liteflow-rule-db-sql`** / **`liteflow-rule-db-postgresql`** / **`liteflow-rule-db-mongodb`** / **`liteflow-rule-db-redis`**：增量 + 轮询模型（seq 轮询 + 周期对账收敛）。
> - **`liteflow-rule-db-zk`** / **`liteflow-rule-db-etcd`**：监听模型（watch 实时推送 + 周期对账收敛）。
> - **`liteflow-rule-db-nacos`**：原子 Catalog + 监听模型（Nacos CAS 整体发布 + Listener 实时推送 + 周期对账收敛）。
>
> 它们不属于 `liteflow-rule-plugin` 下原有的「启动拼 XML」式插件，双方在运行模型与配置上独立。七个 Rule-DB 插件**同一时刻 classpath 只能有一个**（启动时检测到多个会直接报错）；同一后端迁移时也应移除旧规则插件，Nacos 的客户端版本要求见 §5.3。

---

# 上手篇

## 1. 它解决什么

如果你现在用 `liteflow-rule-sql` 或 `liteflow-rule-redis`（或 zk/nacos/etcd/apollo），它们的本质都是同一个流程：

```
启动 → 从存储全量读出所有规则和脚本 → 拼成一个大 XML → 走同一条解析路径加载进 JVM
```

存储在这里只是「启动数据源」。一旦启动完，规则就活在各个节点自己的 JVM 里。这带来两个本质问题：

| 痛点 | 表现 |
|---|---|
| **多节点无一致性保证** | 规则活在各节点 JVM 内，刷新依赖各插件自身的轮询/通知机制，时间窗内各节点版本不一；通知一旦丢失，没有兜底对账，无法保证最终收敛。 |
| **规则与脚本全量常驻 JVM 堆** | 即使开了 `parseOneOnFirstExec`，EL 文本和脚本源码依然全量驻留在 `FlowBus` 的 map 里；`chainCacheEnabled` 只淘汰编译产物，不淘汰文本。规则总量增长直接推高堆内存。 |

Rule-DB 模式把这两件事一次性解决：

1. **存储是权威源，JVM 只是缓存。** 任何写入（发布/删除）都走发布 API，原子完成「更新内容 + 版本号 +1 + 写变更日志」。所有节点通过「**变更通知 + 周期对账**」两条腿收敛，即使通知丢失，对账周期内也必然收敛——一致性语义是**最终收敛、秒级窗口**。变更通知的具体形式随后端而异：
   - **SQL / PostgreSQL / MongoDB / Redis**：seq 序号轮询（默认 3s 一次）。
   - **ZooKeeper / etcd / Nacos**：长连接监听实时推送（毫秒级或亚秒级）。
   - 七者都叠加一条 **周期全量对账**（默认 60s）作为最终兜底。
2. **规则正文与编译产物不再全量常驻。** JVM 仍为每条清单记录维护影子 `Chain` / `Node`、版本戳和状态索引，这部分开销随规则总数线性增长；EL 文本、脚本源码和编译产物进入 **Caffeine 有界缓存**（容量按 chain 条数配），按访问热度淘汰，淘汰后退回「影子」状态，下次执行再懒加载。

一句话划清边界：**老的 6 个插件 = 启动一次性灌库，之后各节点各跑各的；Rule-DB = 存储永远是权威，JVM 只缓存热规则，所有节点最终一致。**

> 「影子状态」是什么？一个 chain 只注册了 `chainId`、没有 EL、未编译；一个脚本 Node 只登记了元数据（type/language/name）、没有源码、未编译。它存在的意义是让索引常驻而内容按需加载——执行到它时才回源拉取并编译。

## 2. 快速上手（SQL）

### Step 1：引入依赖

```xml
<dependency>
    <groupId>com.yomahub</groupId>
    <artifactId>liteflow-spring-boot-starter</artifactId>
    <version>2.16.1</version>
</dependency>
<dependency>
    <groupId>com.yomahub</groupId>
    <artifactId>liteflow-rule-db-sql</artifactId>
    <version>2.16.1</version>
</dependency>
<!-- 数据库驱动用户自带，例如 MySQL -->
<dependency>
    <groupId>com.mysql</groupId>
    <artifactId>mysql-connector-j</artifactId>
</dependency>
```

> Spring Boot 4 项目把 starter 换成 `liteflow-spring-boot4-starter`；Solon 项目用 `liteflow-solon-plugin`。Spring Boot 两个 starter（`liteflow-spring-boot-starter` / `liteflow-spring-boot4-starter`）已内置 `liteflow.rule-db.*` 配置绑定与 IDE 自动补全元数据；Solon 插件支持配置绑定，但**不携带** Spring 风格的 IDE 元数据文件。

> 发布脚本时，执行应用还必须引入对应语言的 LiteFlow 脚本插件。例如 `language("groovy")` 需要 `com.yomahub:liteflow-script-groovy:2.16.1`。Rule-DB 后端模块和 starter 都不会自动引入任何脚本语言实现；只发布普通 chain 时不需要脚本插件。

### Step 2：写配置（三种姿势，按需选最省事的）

**姿势 A：应用已有 DataSource（最省事，零配置）。** 你的 Spring Boot 应用里已经配了数据库连接池（HikariDataSource 等），那只要引依赖、什么都别配——插件会自动复用容器里的 DataSource，`application-name` 自动取 `spring.application.name`。

**姿势 B：规则放独立数据库（三行起步）。** 规则想和应用业务库分开，配三行：

```properties
spring.application.name=order-service
liteflow.rule-db.sql.url=jdbc:mysql://host:3306/liteflow_rules
liteflow.rule-db.sql.username=root
liteflow.rule-db.sql.password=your-password
# driver-class-name 留空，从 url 自动推断
# application-name 留空，自动取 spring.application.name
```

**姿势 C：建表。** 默认 `auto-init-table=false`，你需要自己在数据库建好四张表（DDL 见参考篇 [§7.1](#71-sql-四张表)），缺表启动会报错并附完整 DDL 可直接复制。如果想偷懒，开一个开关：

```properties
liteflow.rule-db.sql.auto-init-table=true
```

启动时插件会执行 `CREATE TABLE IF NOT EXISTS`（表名前缀默认 `lf_`，可用 `table-prefix` 改）。

### Step 3：发布第一条规则

用统一发布 API 写入规则。`applicationName` 是存储隔离维度，必须与执行应用最终解析出的 `liteflow.rule-db.application-name` 一致；下面使用独立 JDBC 配置，管理后台也可以通过 `SqlPublisherConfig.dataSource(...)` 复用自己的连接池。

```java
import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.PublishResult;
import com.yomahub.liteflow.publisher.RulePublisher;
import com.yomahub.liteflow.publisher.RulePublisherFactory;
import com.yomahub.liteflow.repository.sql.SqlPublisherConfig;

try (RulePublisher publisher = RulePublisherFactory.create(
        SqlPublisherConfig.builder()
                .applicationName("order-service")
                .url("jdbc:mysql://host:3306/liteflow_rules")
                .username("root")
                .password("your-password")
                .build())) {
    PublishResult result = publisher.publishChain(PublishChainRequest.builder()
            .chainId("orderChain")
            .el("THEN(a, b)")
            .expectedVersion(0L) // 仅当规则不存在时创建；重复执行会明确报版本冲突
            .build());
    System.out.println("发布成功，当前版本: " + result.getVersion());
}
```

EL 里的 `a`、`b` 是应用内注册的普通 Java 组件。下面两个最小组件分别放入 `ACmp.java` 和 `BCmp.java`，即可跑通本例：

```java
import com.yomahub.liteflow.annotation.LiteflowComponent;
import com.yomahub.liteflow.core.NodeComponent;

@LiteflowComponent("a")
public class ACmp extends NodeComponent {
    @Override
    public void process() {
        System.out.println("a");
    }
}
```

```java
import com.yomahub.liteflow.annotation.LiteflowComponent;
import com.yomahub.liteflow.core.NodeComponent;

@LiteflowComponent("b")
public class BCmp extends NodeComponent {
    @Override
    public void process() {
        System.out.println("b");
    }
}
```

Rule-DB 只纳管 **EL 和脚本**，Java 组件仍随应用代码部署。Publisher 只校验请求字段和存储约束，不会在发布时编译 EL；引用不存在的 Java 组件、子 chain 或脚本节点，会在执行节点首次编译该 chain 时失败。脚本发布和依赖顺序见 [§8.6](#86-发布校验与依赖顺序)。

统一 SQL Publisher 的每次 `publish*` 都在一个**单事务**里先获取 `lf_change_lock` 发布顺序锁，再完成 UPSERT 内容行（`version = version + 1`，重算 md5）和 INSERT 变更日志。锁会持有到提交或回滚，确保变更序号分配顺序与事务提交顺序一致。

### Step 4：执行

应用侧照常执行，API 完全不变：

```java
@Resource
private FlowExecutor flowExecutor;

public void run() {
    LiteflowResponse resp = flowExecutor.execute2Resp("orderChain", null);
    // 首次执行会回源拉取 EL 并编译；二次执行命中缓存，零远程调用
}
```

启动时只读清单（不含内容）建索引，普通 chain 的 EL／脚本内容是**首次执行到时才回源拉取并编译**。命中缓存之后执行热路径零远程调用；路由执行会预先准备所有未就绪 chain 的 route 元数据，冷启动边界见 [§10.3](#103-调优建议)。

## 3. 快速上手（Redis）

### Step 1：引入依赖

```xml
<dependency>
    <groupId>com.yomahub</groupId>
    <artifactId>liteflow-spring-boot-starter</artifactId>
    <version>2.16.1</version>
</dependency>
<dependency>
    <groupId>com.yomahub</groupId>
    <artifactId>liteflow-rule-db-redis</artifactId>
    <version>2.16.1</version>
</dependency>
```

`liteflow-rule-db-redis` 依赖 Redisson，与旧版 `liteflow-rule-redis` 选型一致，运维认知无负担。

### Step 2：写配置（一行起步）

**姿势 A：容器里已有 `RedissonClient` bean。** 那只引依赖、零配置，插件自动复用。

**姿势 B：一行起步。**

```properties
spring.application.name=your-app
liteflow.rule-db.redis.address=redis://127.0.0.1:6379
# 多地址逗号分隔；哨兵模式再加 master-name；集群只填多地址、不配 master-name
# application-name 留空，自动取 spring.application.name
# key-prefix 默认 lf，规则会落在 lf:{app}:... 这组键下
```

Redis 模式**不需要建表**，键结构在首次发布时自动创建（见参考篇 [§7.2](#72-redis-键结构)）。

### Step 3：发布第一条规则

Redis 没有独立简化门面，统一用**发布 API**（`RulePublisherFactory` + `RedisPublisherConfig`）。这个 API **可独立使用**——管理后台只依赖这一个 jar 就能调，不需要拉起 FlowExecutor，也不依赖全局 `LiteflowConfig`：

```java
import com.yomahub.liteflow.publisher.RulePublisher;
import com.yomahub.liteflow.publisher.RulePublisherFactory;
import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.PublishScriptRequest;
import com.yomahub.liteflow.publisher.RemoveRuleRequest;
import com.yomahub.liteflow.repository.redis.RedisPublisherConfig;

try (RulePublisher publisher = RulePublisherFactory.create(
        RedisPublisherConfig.builder()
                .address("redis://127.0.0.1:6379")
                .applicationName("your-app")   // 多应用共库时务必各应用不同
                .build())) {

    // 发布 chain。route / namespace 可选（见下）
    long version = publisher.publishChain(PublishChainRequest.builder()
            .chainId("orderChain")
            .el("THEN(a, b)")
            .build()).getVersion();

    // 带 route（路由 EL）和 namespace 的发布
    publisher.publishChain(PublishChainRequest.builder()
            .chainId("orderChain")
            .el("THEN(a, b)")
            .route("AND(a)")
            .namespace("ns1")
            .build());

    // 发布脚本；执行节点还需引入对应语言的脚本插件
    publisher.publishScript(PublishScriptRequest.builder()
            .nodeId("s1")
            .type("script")
            .language("groovy")
            .script("def a = 1; return a")
            .build());

    // 删除
    publisher.removeChain(RemoveRuleRequest.builder().targetId("orderChain").build());
    publisher.removeScript(RemoveRuleRequest.builder().targetId("s1").build());
}
```

每次发布都是一段 **Lua 脚本原子执行**：HSET 内容 → SADD 索引 → INCR seq → ZADD changelog，四步在 Redis 单线程内原子完成，不会有中间状态被其他客户端看到。返回值（`PublishResult`）含新版本号和变更序号。

> Redis 模式的变更感知**和 SQL 一样靠 seq 轮询**（默认 3s）+ 周期对账（默认 60s）两条腿，**没有** pub/sub 推送通道（详见 [§9](#9-一致性与收敛模型)）。

### Step 4：执行

同 SQL，照常 `flowExecutor.execute2Resp(...)`。

## 4. 快速上手（ZooKeeper）

zk / etcd / Nacos 与 SQL / PostgreSQL / MongoDB / Redis 的区别在于：前三者用**长连接监听实时推送**感知变更（毫秒级或亚秒级），而不是 seq 轮询；周期对账仍作为兜底。

### Step 1：引入依赖

```xml
<dependency>
    <groupId>com.yomahub</groupId>
    <artifactId>liteflow-spring-boot-starter</artifactId>
    <version>2.16.1</version>
</dependency>
<dependency>
    <groupId>com.yomahub</groupId>
    <artifactId>liteflow-rule-db-zk</artifactId>
    <version>2.16.1</version>
</dependency>
```

### Step 2：写配置

```properties
spring.application.name=your-app
liteflow.rule-db.zk.connect-string=127.0.0.1:2181
# 多个地址逗号分隔
liteflow.rule-db.zk.root-path=/liteflow        # 默认 /liteflow
liteflow.rule-db.zk.session-timeout=60000      # 毫秒，默认 60000
# application-name 留空，自动取 spring.application.name
```

### Step 3：发布第一条规则

```java
import com.yomahub.liteflow.publisher.RulePublisher;
import com.yomahub.liteflow.publisher.RulePublisherFactory;
import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.repository.zk.ZkPublisherConfig;

try (RulePublisher publisher = RulePublisherFactory.create(
        ZkPublisherConfig.builder()
                .connectString("127.0.0.1:2181")
                .rootPath("/liteflow")
                .applicationName("your-app")
                .build())) {

    publisher.publishChain(PublishChainRequest.builder()
            .chainId("orderChain")
            .el("THEN(a, b)")
            .build());
}
```

每次发布在**一个 ZooKeeper 事务**（multi-op）内原子完成 meta 节点 + content 节点的写入；节点 `version`（zxid）作变更序号。zk 连接断线/重连时，watch 自动补订阅并触发一次全量对账。

> **部署顺序与最小权限：** 必须先用 Publisher 账号至少创建一次 Publisher，使其初始化 `{root}/{app}/chains/meta`、`chains/content`、`scripts/meta`、`scripts/content` 四棵路径，再启动执行节点。执行侧 Provider 不创建任何 znode，只校验必要路径并读取／watch；因此执行账号只需要这四棵路径及其子节点的递归 `READ` 权限。Publisher 账号需要创建、更新和删除权限。路径缺失时执行节点会 fail-fast，并提示先用 Publisher 初始化。

### Step 4：执行

同前，照常 `flowExecutor.execute2Resp(...)`。

## 5. 快速上手（etcd）

### Step 1：引入依赖

```xml
<dependency>
    <groupId>com.yomahub</groupId>
    <artifactId>liteflow-spring-boot-starter</artifactId>
    <version>2.16.1</version>
</dependency>
<dependency>
    <groupId>com.yomahub</groupId>
    <artifactId>liteflow-rule-db-etcd</artifactId>
    <version>2.16.1</version>
</dependency>
```

### Step 2：写配置

```properties
spring.application.name=your-app
liteflow.rule-db.etcd.endpoints=http://127.0.0.1:2379
# 多个 endpoint 逗号分隔
liteflow.rule-db.etcd.root-path=/liteflow       # 默认 /liteflow
# liteflow.rule-db.etcd.user=                   # 可选，etcd 鉴权用户名
# liteflow.rule-db.etcd.password=               # 可选
```

### Step 3：发布第一条规则

```java
import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.RulePublisher;
import com.yomahub.liteflow.publisher.RulePublisherFactory;
import com.yomahub.liteflow.repository.etcd.EtcdPublisherConfig;

try (RulePublisher publisher = RulePublisherFactory.create(
        EtcdPublisherConfig.builder()
                .endpoints("http://127.0.0.1:2379")
                .rootPath("/liteflow")
                .applicationName("your-app")
                .build())) {

    publisher.publishChain(PublishChainRequest.builder()
            .chainId("orderChain")
            .el("THEN(a, b)")
            .build());
}
```

etcd 用 **KV revision** 作变更序号，watch 按 revision 区间订阅。etcd 对历史 revision 有 compaction 上限——一旦 watch 因 revision 被 compact 而失败，会自动降级为全量对账后重新续上 watch。

### Step 4：执行

同前，照常 `flowExecutor.execute2Resp(...)`。

## 5.1 快速上手（PostgreSQL）

引入 starter 和 PostgreSQL 后端模块：

```xml
<dependency>
    <groupId>com.yomahub</groupId>
    <artifactId>liteflow-spring-boot-starter</artifactId>
    <version>2.16.1</version>
</dependency>
<dependency>
    <groupId>com.yomahub</groupId>
    <artifactId>liteflow-rule-db-postgresql</artifactId>
    <version>2.16.1</version>
</dependency>
```

配置独立 JDBC 地址；如果容器中已有 `DataSource`，也可以省略连接配置并自动复用：

```properties
spring.application.name=your-app
liteflow.rule-db.postgresql.url=jdbc:postgresql://127.0.0.1:5432/liteflow
liteflow.rule-db.postgresql.username=postgres
liteflow.rule-db.postgresql.password=your-password
liteflow.rule-db.postgresql.auto-init-table=true
```

独立发布程序使用 `PostgresqlPublisherConfig`：

```java
import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.RulePublisher;
import com.yomahub.liteflow.publisher.RulePublisherFactory;
import com.yomahub.liteflow.repository.postgresql.PostgresqlPublisherConfig;

try (RulePublisher publisher = RulePublisherFactory.create(
        PostgresqlPublisherConfig.builder()
                .applicationName("your-app")
                .url("jdbc:postgresql://127.0.0.1:5432/liteflow")
                .username("postgres")
                .password("your-password")
                .build())) {
    publisher.publishChain(PublishChainRequest.builder()
            .chainId("orderChain").el("THEN(a, b)").build());
}
```

PostgreSQL 使用数据库事务原子提交内容、业务版本和 `change_log`，变更通过 seq 轮询加周期对账收敛。

## 5.2 快速上手（MongoDB）

引入 starter 和 MongoDB 后端模块：

```xml
<dependency>
    <groupId>com.yomahub</groupId>
    <artifactId>liteflow-spring-boot-starter</artifactId>
    <version>2.16.1</version>
</dependency>
<dependency>
    <groupId>com.yomahub</groupId>
    <artifactId>liteflow-rule-db-mongodb</artifactId>
    <version>2.16.1</version>
</dependency>
```

配置 MongoDB URI；容器中已有 `MongoClient` bean 时可以不配 URI：

```properties
spring.application.name=your-app
liteflow.rule-db.mongodb.uri=mongodb://127.0.0.1:27017/?replicaSet=rs0
liteflow.rule-db.mongodb.database=liteflow
```

独立发布程序使用 `MongoPublisherConfig`：

```java
import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.RulePublisher;
import com.yomahub.liteflow.publisher.RulePublisherFactory;
import com.yomahub.liteflow.repository.mongodb.MongoPublisherConfig;

try (RulePublisher publisher = RulePublisherFactory.create(
        MongoPublisherConfig.builder()
                .applicationName("your-app")
                .uri("mongodb://127.0.0.1:27017/?replicaSet=rs0")
                .database("liteflow")
                .build())) {
    publisher.publishChain(PublishChainRequest.builder()
            .chainId("orderChain").el("THEN(a, b)").build());
}
```

MongoDB 后端使用多文档事务原子发布规则，并使用快照事务读取一致的 Manifest 和序号基线，因此整个后端都要求**副本集或分片集群**；standalone MongoDB 不受支持。

## 5.3 快速上手（Nacos）

Nacos Rule-DB 依赖 `publishConfigCas` 保证并发发布的原子性，因此要求 **Nacos Server 2.x 或更高版本**。该模块显式使用 `nacos-client:2.5.3`，旧 `liteflow-rule-nacos` 插件仍使用 1.4.4；迁移时不要把两个模块同时放进同一 classpath。若外部 BOM 把客户端降级为不含 CAS API 的版本，模块会在初始化时 fail-fast。

### Step 1：引入依赖

```xml
<dependency>
    <groupId>com.yomahub</groupId>
    <artifactId>liteflow-spring-boot-starter</artifactId>
    <version>2.16.1</version>
</dependency>
<dependency>
    <groupId>com.yomahub</groupId>
    <artifactId>liteflow-rule-db-nacos</artifactId>
    <version>2.16.1</version>
</dependency>
```

### Step 2：写配置

```properties
spring.application.name=order-service
liteflow.rule-db.nacos.server-addr=127.0.0.1:8848
# namespace 填命名空间 ID，不是显示名称；留空使用 public
# liteflow.rule-db.nacos.namespace=your-namespace-id
# group 默认 LITEFLOW_RULE_DB
# data-id-prefix 默认 liteflow-rule-db
# application-name 留空，自动取 spring.application.name
```

运行时会把当前应用映射为一条 Nacos 配置：

```text
dataId = {data-id-prefix}.{application-name}.catalog.json
group  = {group}
```

例如应用名为 `order-service` 时，默认 `dataId` 是 `liteflow-rule-db.order-service.catalog.json`，默认 group 是 `LITEFLOW_RULE_DB`。容器里已有 `ConfigService` bean 时可不配 `server-addr`；多 bean 场景用 `config-service-bean-name` 精确指定。

### Step 3：发布第一条规则

```java
import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.RulePublisher;
import com.yomahub.liteflow.publisher.RulePublisherFactory;
import com.yomahub.liteflow.repository.nacos.NacosPublisherConfig;

try (RulePublisher publisher = RulePublisherFactory.create(
        NacosPublisherConfig.builder()
                .serverAddr("127.0.0.1:8848")
                .applicationName("order-service")
                .build())) {
    publisher.publishChain(PublishChainRequest.builder()
            .chainId("orderChain")
            .el("THEN(a, b)")
            .expectedVersion(0L)
            .build());
}
```

每次发布先读取当前 Catalog，再通过 Nacos CAS 整体替换。Catalog 内的正文、业务版本、全局 sequence 和 `lastChange` 在同一次 CAS 中提交；并发写入冲突会重新读取后重试，显式 `expectedVersion` 已失效时抛 `VersionConflictException`。执行节点通过 Nacos Listener 感知 `lastChange`，发现序号跳跃、损坏内容或回调失败时立即请求全量对账。

> **容量前置评估：** 一个应用的全部 chain 与脚本正文都在同一条 Nacos 配置中，受 Nacos 服务端、数据库字段和接入层的单配置大小限制。上线前必须用生产等价配置压测 Catalog 最大体积并预留增长空间；规则规模大、正文很长或发布频繁时，优先选择 SQL / PostgreSQL / MongoDB。执行端不会长期保留整份 Catalog 正文，但每次冷读取和对账都需要传输并解码整份配置。

### Step 4：执行

同前，照常 `flowExecutor.execute2Resp(...)`。

---

上手篇到此结束。下面参考篇是逐项细节，按需查阅。

---

# 参考篇

## 6. 配置参考

所有 Rule-DB 配置都在 `liteflow.rule-db.*` 命名空间下，绑定到 `com.yomahub.liteflow.property.RuleDbConfig`。配置是**嵌套结构**：通用项在 `liteflow.rule-db.*`，缓存项在 `liteflow.rule-db.cache.*`，同步项在 `liteflow.rule-db.sync.*`，各后端专属项分别位于 `.sql.*` / `.postgresql.*` / `.mongodb.*` / `.redis.*` / `.zk.*` / `.etcd.*` / `.nacos.*`。

### 通用配置（七个后端共用）

| 配置项 | 默认 | 说明 |
|---|---|---|
| `liteflow.rule-db.enabled` | `true` | 引入依赖即激活；这是逃生开关，设 `false` 则退回非 Rule-DB 行为。 |
| `liteflow.rule-db.application-name` | Spring Boot 应用自动取 `spring.application.name` | 多应用共库的隔离维度。同一套存储里不同 `application-name` 的规则互不可见。非 Spring / Solon 环境或未配 `spring.application.name` 时回落为 `default`——**多应用共库时务必保证各应用取值不同**，否则会互相读写对方的规则。 |
| `liteflow.rule-db.cache.capacity` | `500` | Caffeine 有界缓存容量（按 chain 条数计）。超出后按访问热度淘汰，淘汰的 chain 退回影子状态，其引用的脚本引用计数减一。 |
| `liteflow.rule-db.cache.preload-chain-ids` | 空 | 启动预热的 chain id 列表（逗号分隔）。关键链路建议列在这里，抹平冷启动的首次回源尖刺。预热失败只记一条 warn、不会阻断启动。 |
| `liteflow.rule-db.sync.poll-seconds` | `3`（SQL / PostgreSQL / MongoDB / Redis） | 变更序号轮询周期；zk / etcd / Nacos 用监听，该项对它们不生效。 |
| `liteflow.rule-db.sync.reconcile-seconds` | `60` | 清单对账周期，全量 diff 索引与缓存。无论通知还是轮询都丢了的极端情况下，这个周期是收敛的最终保证。 |
| `liteflow.rule-db.sync.fetch-retry-times` | `3` | 回源拉取失败的重试次数。 |

### SQL 专属配置（`liteflow-rule-db-sql`）

| 配置项 | 默认 | 说明 |
|---|---|---|
| `liteflow.rule-db.sql.url` | — | JDBC url。**不配**则自动查找容器里的 `DataSource` bean 复用。 |
| `liteflow.rule-db.sql.username` | — | 配合 `url` 使用。 |
| `liteflow.rule-db.sql.password` | — | 配合 `url` 使用。 |
| `liteflow.rule-db.sql.driver-class-name` | 从 `url` 自动推断 | 留空即可。 |
| `liteflow.rule-db.sql.datasource-bean-name` | 自动查找 | 多 `DataSource` 场景下，用它指定复用哪个 bean。 |
| `liteflow.rule-db.sql.table-prefix` | `lf_` | 表名前缀，可改；只允许 ASCII 字母、数字、下划线，最长 54 个字符，字段名固定不可配。 |
| `liteflow.rule-db.sql.auto-init-table` | `false` | 设 `true` 则启动时 `CREATE TABLE IF NOT EXISTS`。 |
| `liteflow.rule-db.sql.change-log-batch-size` | `1000` | 每次轮询最多读取的变更日志条数。积压会按批次连续排空，避免一次把全部历史记录载入内存。必须大于 0。 |

> **生产建议用容器 DataSource（连接池）。** 走 `url` 直连时，框架用 `DriverManager` 裸连接，每次回源都建连、无池化——仅适合开发/测试。生产环境配好 HikariCP 等连接池的 `DataSource` bean，让插件复用（姿势 A）。

> SQL 插件当前发布支持矩阵为 **MySQL / MariaDB**；PostgreSQL 请使用独立的 `liteflow-rule-db-postgresql`。Oracle、SQL Server 等数据库不在本版本支持范围内。

### PostgreSQL 专属配置（`liteflow-rule-db-postgresql`）

| 配置项 | 默认 | 说明 |
|---|---|---|
| `liteflow.rule-db.postgresql.url` | — | PostgreSQL JDBC URL；不配则自动查找容器 `DataSource`。 |
| `liteflow.rule-db.postgresql.username` / `.password` | — | JDBC 认证信息。 |
| `liteflow.rule-db.postgresql.driver-class-name` | `org.postgresql.Driver` | 通常无需配置。 |
| `liteflow.rule-db.postgresql.datasource-bean-name` | 自动查找 | 多数据源时指定 bean。 |
| `liteflow.rule-db.postgresql.table-prefix` | `lf_` | 表名前缀；只允许 ASCII 字母、数字、下划线，最长 52 个字符。 |
| `liteflow.rule-db.postgresql.auto-init-table` | `false` | 是否执行模块内 PostgreSQL DDL。 |
| `liteflow.rule-db.postgresql.change-log-batch-size` | `1000` | 单批变更日志上限。 |

### MongoDB 专属配置（`liteflow-rule-db-mongodb`）

| 配置项 | 默认 | 说明 |
|---|---|---|
| `liteflow.rule-db.mongodb.uri` | — | MongoDB URI；不配则自动查找容器 `MongoClient`；必须连接副本集或分片集群。 |
| `liteflow.rule-db.mongodb.database` | `liteflow` | 数据库名。 |
| `liteflow.rule-db.mongodb.collection-prefix` | `lf_` | Collection 前缀。 |
| `liteflow.rule-db.mongodb.mongo-client-bean-name` | 自动查找 | 多 `MongoClient` 时指定 bean。 |
| `liteflow.rule-db.mongodb.change-log-batch-size` | `1000` | 单批变更日志文档上限。 |

### Redis 专属配置（`liteflow-rule-db-redis`）

| 配置项 | 默认 | 说明 |
|---|---|---|
| `liteflow.rule-db.redis.address` | — | 单机/哨兵/集群统一入口，多地址逗号分隔（如 `redis://h1:6379,redis://h2:6379`）。 |
| `liteflow.rule-db.redis.master-name` | — | **配置即哨兵模式**；不配则按地址数自动推断单机/集群。 |
| `liteflow.rule-db.redis.username` | — | Redis 6+ ACL 用户名，可空（配合 `address` 使用；单机/哨兵/集群均生效）。 |
| `liteflow.rule-db.redis.password` | — | Redis 口令，可空（配合 `address` 使用）。 |
| `liteflow.rule-db.redis.database` | `0` | Redis 逻辑库。 |
| `liteflow.rule-db.redis.key-prefix` | `lf` | 键前缀。规则落在 `{prefix}:{app}:...` 下。 |
| `liteflow.rule-db.redis.key-hash-tag` | — | Redis Cluster hash-tag（不含花括号）。配置后所有键落在 `{prefix}:{hashTag}:{app}:...` 下、固定到同一 slot，Lua 多键原子发布才能工作。**多地址且无 `master-name`（cluster 模式）时必填**，缺失会在建连时抛 `ConfigErrorException`；单机/哨兵模式可不配。 |
| `liteflow.rule-db.redis.redisson-bean-name` | 自动查找 | 容器有 `RedissonClient` bean 时复用，不必再配 `address`；此时鉴权在该 bean 上配置。 |

### ZooKeeper 专属配置（`liteflow-rule-db-zk`）

| 配置项 | 默认 | 说明 |
|---|---|---|
| `liteflow.rule-db.zk.connect-string` | — | zk 集群地址，多地址逗号分隔。 |
| `liteflow.rule-db.zk.root-path` | `/liteflow` | 根路径，所有规则节点挂在 `{root}/{app}/...` 下。 |
| `liteflow.rule-db.zk.session-timeout` | `60000` | 会话超时（毫秒）。 |
| `liteflow.rule-db.zk.username` | — | digest 认证用户名，可空。配置后模块创建的所有 znode 使用 `CREATOR_ALL_ACL`（仅持有该 digest 身份的连接可写）。与 `password` 必须成对配置，且用户名不允许包含 `:`，否则启动时抛 `ConfigErrorException`。 |
| `liteflow.rule-db.zk.password` | — | digest 认证口令，与 `username` 成对配置。 |
| `liteflow.rule-db.zk.curator-bean-name` | — | 复用容器中已有 `CuratorFramework` bean 的名字；配置后 `connect-string` / `session-timeout` / `username` / `password` 不再生效（连接与认证由该 bean 自身配置决定）。bean 不存在时启动抛 `ConfigErrorException`。 |

> **单条规则大小受 zk 单 znode 1MB（`jute.maxbuffer`）限制。** chain EL 或脚本源码的 content znode 超过约 1MB 会被 ZK 服务端拒绝，发布失败；模块会在发布前对编码后内容做大小预检（阈值 960KB），超限抛出 `RuleValidationException`，不会写入 ZK。超大规则请拆分为多条 chain/脚本，或改用 SQL / PostgreSQL / MongoDB 后端。

### etcd 专属配置（`liteflow-rule-db-etcd`）

| 配置项 | 默认 | 说明 |
|---|---|---|
| `liteflow.rule-db.etcd.endpoints` | — | etcd endpoint 列表，逗号分隔，必须带 `http://` 或 `https://` scheme，不允许混用两种 scheme；格式非法或为空时启动抛 `ConfigErrorException`。 |
| `liteflow.rule-db.etcd.root-path` | `/liteflow` | 根路径。 |
| `liteflow.rule-db.etcd.user` | — | 可选，etcd 鉴权用户名。与 `password` 必须成对配置。 |
| `liteflow.rule-db.etcd.password` | — | 可选，etcd 鉴权口令。 |
| `liteflow.rule-db.etcd.ca-certificate` | — | 可选，CA 证书文件路径（PEM），用于校验 etcd 服务端证书；仅在 endpoints 为 `https://` 时生效，必须是可读文件。 |
| `liteflow.rule-db.etcd.client-certificate` | — | 可选，客户端证书文件路径（mTLS）；与 `client-key` 必须成对配置，仅在 `https://` endpoints 下生效。 |
| `liteflow.rule-db.etcd.client-key` | — | 可选，客户端私钥文件路径（mTLS）；与 `client-certificate` 成对配置。 |
| `liteflow.rule-db.etcd.authority` | — | 可选，gRPC authority 覆盖值（TLS 下用于指定虚拟主机名/证书域名不匹配时的目标名）。 |
| `liteflow.rule-db.etcd.connect-timeout-millis` | `5000` | 连接超时（毫秒），必须大于 0。 |
| `liteflow.rule-db.etcd.keepalive-time-seconds` | `30` | gRPC keepalive 探测间隔（秒），必须大于 0。 |
| `liteflow.rule-db.etcd.keepalive-timeout-seconds` | `10` | keepalive 探测超时（秒），必须大于 0。 |
| `liteflow.rule-db.etcd.keepalive-without-calls` | `true` | 无活跃调用时是否仍发送 keepalive 探测。 |
| `liteflow.rule-db.etcd.client-bean-name` | — | 复用容器中已有 `io.etcd.jetcd.Client` bean 的名字；配置后以上连接/认证/TLS 配置均不生效。bean 不存在时启动抛 `ConfigErrorException`。 |

#### TLS / mTLS

etcd 后端的 TLS 由 endpoint scheme 驱动：

- **单向 TLS**：endpoints 全部使用 `https://`，按需配置 `ca-certificate` 指定私有 CA；不配则用 JVM 默认信任库。
- **双向 TLS（mTLS）**：在单向 TLS 基础上，成对配置 `client-certificate` + `client-key`，供开启了客户端证书校验的 etcd 集群认证。
- 以下配置错误都会在启动时 fail-fast（`ConfigErrorException`）：http/https endpoint 混用；`user`/`password` 只配一个；`client-certificate`/`client-key` 只配一个；在 `http://` endpoints 下配置任何证书；证书文件不存在或不可读；超时/keepalive 参数 ≤ 0。

### Nacos 专属配置（`liteflow-rule-db-nacos`）

| 配置项 | 默认 | 说明 |
|---|---|---|
| `liteflow.rule-db.nacos.server-addr` | — | Nacos 地址；不配则自动查找容器中的 `ConfigService` bean。要求连接 Nacos Server 2.x 或更高版本。 |
| `liteflow.rule-db.nacos.namespace` | public | Nacos namespace ID；不要填写控制台显示名称。 |
| `liteflow.rule-db.nacos.group` | `LITEFLOW_RULE_DB` | Catalog 所在 group；只允许 Nacos 支持的字母、数字、下划线、连字符、点和冒号。 |
| `liteflow.rule-db.nacos.data-id-prefix` | `liteflow-rule-db` | Catalog 的 dataId 前缀，最终 dataId 为 `{prefix}.{applicationName}.catalog.json`。字符约束同 group。 |
| `liteflow.rule-db.nacos.username` / `.password` | — | Nacos 用户名和口令，必须成对配置。 |
| `liteflow.rule-db.nacos.access-key` / `.secret-key` | — | Nacos AK/SK，必须成对配置；不能与 username/password 同时配置。 |
| `liteflow.rule-db.nacos.timeout-millis` | `3000` | 配置读取和监听注册超时（毫秒），必须大于 0。 |
| `liteflow.rule-db.nacos.config-service-bean-name` | 自动查找 | 复用容器中已有 `ConfigService` bean 的名字；未指定时按类型自动查找。复用的 bean 归容器所有，Provider 关闭时不会调用其 `shutDown()`。 |

独立 Publisher 通过 `NacosPublisherConfig.configService(...)` 复用外部客户端，所有权规则相同；未传客户端时由 Publisher 创建并在 `close()` 时关闭。显式 bean／客户端已包含连接与认证配置，此时 `server-addr` 等建连参数不参与创建。

### 与旧配置的关系

进入 Rule-DB 模式后，以下旧配置**不再适用**，由 `rule-db.*` 接管语义：

- `liteflow.rule-source` —— **互斥**，同时配置会启动报错 `rule-source and rule-db mode cannot be used together, please remove one of them`。
- `parseMode`、`enableMonitorFile`、`chainCacheEnabled` / `chainCacheCapacity` —— Rule-DB 路径不读取这些配置（解析时机、热重载、缓存语义均由 `rule-db.*` 接管），配置了也没有效果，建议从配置文件里删掉以免误导后人。

---

## 7. 存储结构参考

### 7.1 SQL 四张表

DDL 随 `liteflow-rule-db-sql` 模块提供：[`liteflow-rule-db/liteflow-rule-db-sql/src/main/resources/sql/ddl-mysql.sql`](../liteflow-rule-db/liteflow-rule-db-sql/src/main/resources/sql/ddl-mysql.sql)。表名前缀可配（默认 `lf_`），字段名固定。

**`lf_chain`** — 主键 (`application_name`, `chain_id`)

| 列 | 类型 | 说明 |
|---|---|---|
| `application_name` | VARCHAR(64) | 应用隔离维度 |
| `chain_id` | VARCHAR(128) | chain 标识 |
| `namespace` | VARCHAR(64) NULL | 命名空间 |
| `el_data` | TEXT | EL 表达式 |
| `route_data` | TEXT NULL | 路由 EL（route chain 用） |
| `version` | BIGINT | 每次发布 +1 |
| `content_md5` | CHAR(32) | `el_data` 的 MD5（**不含** `route_data`，对齐 Publisher 的 `SecureUtil.md5(el)`） |
| `enable` | TINYINT | 1 启用 / 0 停用 |
| `gmt_create` / `gmt_modified` | DATETIME | 创建/修改时间 |

**`lf_script`** — 主键 (`application_name`, `node_id`)

| 列 | 类型 | 说明 |
|---|---|---|
| `application_name` | VARCHAR(64) | 应用隔离维度 |
| `node_id` | VARCHAR(128) | 脚本节点标识 |
| `script_name` | VARCHAR(128) NULL | 节点名 |
| `script_type` | VARCHAR(32) | 对齐 `NodeTypeEnum` 的脚本类型：`script` / `boolean_script` / `switch_script` / `for_script`（WHILE/ITERATOR 是非脚本节点类型，无 `*_script` 变体） |
| `script_language` | VARCHAR(32) NULL | `groovy` / `js` / `python` …，为空用全局默认 |
| `script_data` | TEXT | 脚本源码 |
| `content_md5` | CHAR(32) | `script_data` 的 MD5（对齐 Publisher 的 `SecureUtil.md5(script_data)`） |
| `version` / `enable` / `gmt_create` / `gmt_modified` | | 同上 |

**`lf_change_log`** — 主键 `seq` AUTO_INCREMENT，索引 (`application_name`, `seq`)

| 列 | 说明 |
|---|---|
| `seq` | 整张表全局单调递增的变更序号；不同应用共享序号空间，跨应用跳号正常。 |
| `application_name` | 应用隔离维度 |
| `target_type` | `CHAIN` / `SCRIPT` |
| `target_id` | `chainId` / `nodeId` |
| `op` | `UPSERT` / `DELETE` |
| `version` | 变更后的版本号 |
| `gmt_create` | 创建时间 |

**`lf_change_lock`** —— 单行发布顺序锁

| 列 | 说明 |
|---|---|
| `lock_id` | 主键；必须存在且仅使用值 `1`。Publisher 在事务内执行 `SELECT ... FOR UPDATE`，并持锁到提交或回滚。 |

该锁把 change log 序号的分配顺序与发布事务的提交顺序对齐，避免并发事务先拿到较小 seq 却后提交，导致轮询节点越过尚未提交的变更。它是整套表的全局锁，不按 `application_name` 拆分。

`change_log` 允许运维定期清理（建议保留 7 天），但 SQL / PostgreSQL 使用跨应用共享的全局序号，运行时不会通过查询 `MIN(seq)` 判断每一个缺口。水位前进却读不到本应用记录时会请求全量对账；如果裁剪后仍能读到更晚的本应用记录，缺失变化可能要等下一次周期对账才补齐。因此清理不破坏最终正确性，但可能让个别节点失去 seq 轮询的快速收敛路径；清理窗口应明显大于节点最长离线时间，并保留周期对账。

#### 已有 SQL 部署升级

从没有 `change_lock` 的旧版本升级时，必须在恢复发布流量前执行以下迁移。示例使用默认前缀 `lf_`；自定义 `table-prefix` 时同步替换表名。

```sql
CREATE TABLE IF NOT EXISTS `lf_change_lock` (
  `lock_id` TINYINT NOT NULL,
  PRIMARY KEY (`lock_id`)
) DEFAULT CHARACTER SET utf8mb4;
INSERT IGNORE INTO `lf_change_lock` (`lock_id`) VALUES (1);
```

表存在但 `lock_id = 1` 行缺失同样会使发布失败。不要删除或更新这行数据。

### 7.2 Redis 键结构

键前缀可配（默认 `lf`），`{app}` 为 `application-name`。配置了 `key-hash-tag` 时键布局变为 `{prefix}:{hashTag}:{app}:...`（Redis Cluster 必填，见 [§13 限制 3](#13-限制与已知边界)）。所有键都没有显式设置过期——规则的权威源不会被自动清理，删除走 `removeChain` / `removeScript`。

| 键 | 类型 | 内容 |
|---|---|---|
| `{prefix}:{app}:chain:{chainId}` | HASH | 字段：`el` / `route` / `namespace` / `version` / `md5` / `enable` |
| `{prefix}:{app}:script:{nodeId}` | HASH | 字段：`script` / `name` / `type` / `language` / `version` / `md5` / `enable` |
| `{prefix}:{app}:chain-ids` | SET | 所有 chain 的 id 集合（只存 id，不存版本） |
| `{prefix}:{app}:script-ids` | SET | 所有 script 的 id 集合 |
| `{prefix}:{app}:seq` | STRING | 全局变更序号（`INCR` 自增） |
| `{prefix}:{app}:changelog` | ZSET | `score = seq`，`member = JSON{seq, targetType, targetId, op, version}` |

两个 `*-ids` SET 是有意设计的：清单/对账先 `readAll` 拿到全部 id 集合，再用一个 pipeline（`RBatch`）批量读取每个 chain/script 的元数据 HASH，一次往返拿齐 id + 版本，不必逐个扫描内容键。版本指纹存在各自的内容 HASH 里（`version`/`md5` 字段），不存在索引键上。

> **changelog 需要运维定期裁剪。** `changelog` ZSet 每次发布都会 `ZADD` 一条且框架不会自动收缩，长期高频发布会持续占用内存。建议运维定期执行（如保留最近 7 天或最近 N 条）：
>
> ```
> ZREMRANGEBYSCORE lf:{app}:changelog 0 {要清理到的seq}
> ```
>
> 裁剪不影响正确性：节点发现自己的位点低于 ZSet 中最小 seq（断档）时，会自动触发一次全量对账（与 SQL 的 `change_log` 清理同一套自愈机制）。

### 7.3 ZooKeeper 路径结构

所有规则节点挂在 `{root}/{app}/...` 下（`root` 默认 `/liteflow`）。每个 chain / script 拆成**两个 znode**：`meta` 存轻量指纹供清单遍历，`content` 存全文按需拉取。

| 路径 | 内容 |
|---|---|
| `{root}/{app}/chains/meta/{chainId}` | chain 元数据（version / md5 / enable 等） |
| `{root}/{app}/chains/content/{chainId}` | chain EL 全文（带版本） |
| `{root}/{app}/scripts/meta/{nodeId}` | script 元数据 |
| `{root}/{app}/scripts/content/{nodeId}` | script 源码全文 |

清单只遍历 `meta` 子节点（拿 id + 版本），content 节点在懒加载时才读。变更序号用 **zxid**（节点的 mzxid）——所以 zk 后端没有独立的 changelog/seq 节点，watch 直接监听 meta 节点的增删改，每个事件自带 zxid 作为序号。

> zk 的 znode 数随规则数线性增长；规则量大时注意 zk 的 znode/quota 限制。zk 没有像 SQL `change_log` 那样需要清理的日志结构。

### 7.4 etcd 键结构

布局与 zk 同构，只是把 znode 换成 etcd KV（前缀 key 而非层级节点）：

| key 前缀 | 内容 |
|---|---|
| `{root}/{app}/chains/meta/{chainId}` | chain 元数据 |
| `{root}/{app}/chains/content/{chainId}` | chain EL 全文 |
| `{root}/{app}/scripts/meta/{nodeId}` | script 元数据 |
| `{root}/{app}/scripts/content/{nodeId}` | script 源码全文 |

变更序号用 etcd 的 **KV revision**。watch 按 revision 区间订阅 meta 前缀。etcd 会定期 compact 历史 revision——一旦节点位点落后到已 compact 的 revision，watch 会报 revision compacted 错误，此时自动降级为一次全量对账后重新从最新 revision 续上 watch。和 zk 一样，etcd 后端没有独立 changelog，不需要清理日志。

### 7.5 PostgreSQL 四张表

PostgreSQL 使用 `chain`、`script`、`change_log`、`change_lock` 四张表，逻辑字段与 SQL 后端一致，但 DDL 使用 `BIGSERIAL`、`BOOLEAN` 和 `TIMESTAMPTZ`。完整 DDL 位于 [`postgresql/ddl.sql`](../liteflow-rule-db/liteflow-rule-db-postgresql/src/main/resources/postgresql/ddl.sql)。Publisher 在同一事务中锁定 `change_lock.lock_id = 1`、写正文并通过 `INSERT ... RETURNING seq` 取得变更序号；该行锁同样保证 seq 分配顺序与提交顺序一致。

已有 PostgreSQL 部署必须在恢复发布流量前执行以下迁移。示例使用默认前缀 `lf_`；自定义 `table-prefix` 时同步替换表名。

```sql
CREATE TABLE IF NOT EXISTS lf_change_lock (
  lock_id SMALLINT PRIMARY KEY
);
INSERT INTO lf_change_lock (lock_id) VALUES (1)
ON CONFLICT (lock_id) DO NOTHING;
```

表存在但 `lock_id = 1` 行缺失同样会使发布失败。不要删除或更新这行数据。

### 7.6 MongoDB Collection

默认使用以下四个 Collection：

| Collection | 内容 |
|---|---|
| `lf_chain` | chain 元数据与正文，`_id` 由 applicationName + chainId 组成。 |
| `lf_script` | script 元数据与正文。 |
| `lf_sequence` | 每个 applicationName 独立的连续 seq。 |
| `lf_change_log` | `seq`、目标类型、目标 id、操作和业务版本。 |

Publisher 初始化时负责建立 Manifest 与变更轮询所需索引：`lf_chain.applicationName`、`lf_script.applicationName`，以及唯一复合索引 `lf_change_log(applicationName, seq)`。执行侧 Provider 为支持最小权限账号，启动时不会创建 Collection 或索引；只部署执行节点时，必须先运行一次 Publisher 初始化，或由 DBA 等价地预建这些索引。

Manifest 查询只投影元数据字段，不读取 EL／脚本正文，并通过快照事务保证元数据与 sequence 基线一致。Publisher 在一个 MongoDB 多文档事务里同时更新内容、sequence 和 change log；因此运行与发布都必须连接支持事务的副本集或分片集群。

### 7.7 Nacos Catalog

Nacos 后端按 applicationName 存一条 JSON Catalog：

```text
dataId = {data-id-prefix}.{application-name}.catalog.json
group  = {group}
```

Catalog 的逻辑结构如下，数组内保存完整的 chain／script 记录：

```json
{
  "schemaVersion": 1,
  "sequence": 2,
  "chains": [
    {
      "chainId": "orderChain",
      "el": "THEN(a,b)",
      "route": null,
      "namespace": null,
      "version": 1,
      "md5": "...",
      "enable": true
    }
  ],
  "scripts": [],
  "lastChange": {
    "seq": 2,
    "targetType": "CHAIN",
    "targetId": "orderChain",
    "op": "UPSERT",
    "version": 1
  }
}
```

`sequence` 是应用级连续序号，`lastChange.seq` 必须与之相等。读取端会严格校验 schema、字段类型、重复 id、正文 MD5、业务版本以及 `lastChange` 与最终记录是否一致；Catalog 损坏、序号倒退或内容变化但序号不变都会被视为存储错误，而不是静默接受。

Publisher 使用当前 Nacos 配置 MD5 作为 CAS 条件，整体替换 Catalog，因此正文、Manifest、业务版本与变更序号不存在跨配置的中间态。Listener 只需传递最新 `lastChange`；如果 Nacos 合并了连续回调，执行节点会检测到 sequence 断档并转为全量对账。

执行端常驻快照只保存 `sequence + Catalog MD5`，不会把 `chains` / `scripts` 正文长期留在 Provider 内；但 Nacos 的读取粒度仍是整份配置，Manifest 对账和每次缓存未命中的正文读取都会传输、校验并短暂解码整个 Catalog。因此该后端面向中小规模规则集，容量与吞吐评估必须按“单应用整份 Catalog”进行。

---

## 8. 发布协议与写入规范

### 8.1 推荐：统一发布 API

七个后端共用一套发布接口 `com.yomahub.liteflow.publisher.RulePublisher`，通过 `RulePublisherFactory.create(config)` 按你传入的后端配置实例化。**这是推荐写入方式**，尤其适合独立的管理后台（只依赖一个插件 jar、不拉起 FlowExecutor、不依赖全局 `LiteflowConfig`）。

```java
// 以 Redis 为例；其他后端换成对应的 XxxPublisherConfig
try (RulePublisher publisher = RulePublisherFactory.create(
        RedisPublisherConfig.builder()
                .address("redis://127.0.0.1:6379")
                .applicationName("your-app")
                .build())) {

    PublishResult r = publisher.publishChain(PublishChainRequest.builder()
            .chainId("orderChain")
            .el("THEN(a, b)")
            .route("AND(a)")          // 可选：路由 EL
            .namespace("ns1")         // 可选：命名空间
            .build());
    r.getVersion();   // 新版本号
    r.getSequence();  // 变更序号（SQL/PostgreSQL/MongoDB/Redis/Nacos seq，zk zxid，etcd revision）
}
```

三个请求类型都是不可变 builder 对象，另外返回一个 `PublishResult`：

- `PublishChainRequest`：`chainId` / `el` / `route`(可空) / `namespace`(可空) / `expectedVersion`(可空)。
- `PublishScriptRequest`：`nodeId` / `script` / `name`(可空) / `type`（`script`/`boolean_script`/`switch_script`/`for_script`） / `language`(可空) / `expectedVersion`(可空)。发布时框架自算 md5，`version` 由存储层自增。
- `RemoveRuleRequest`：`targetId` / `expectedVersion`(可空)。`removeChain` / `removeScript` 共用。
- 返回 `PublishResult`：`targetId` / `targetType` / `operation`(`UPSERT`/`DELETE`) / `version` / `sequence`。

发布脚本只负责保存源码和语言标识；每个执行应用都必须显式引入对应的 `liteflow-script-*` 插件，否则该脚本首次加载时会失败。

**乐观锁 `expectedVersion`（并发安全发布的关键）：**

- **不设**（`null`，默认）：UPSERT 语义。已存在则 `version = version + 1`，不存在则插入 `version = 1`。已有行的并发更新在行锁/Lua/事务下原子自增，天然安全。
- **设 `0`**：表示「必须是新建」。若目标已存在，抛 `VersionConflictException`。**并发首发同一个 id 时用它**：第一个成功，其余被明确拒绝，不会重复插入或丢更新。
- **设 `N`（>0）**：CAS 语义。仅当目标当前版本恰为 `N` 时才更新到 `N+1`，否则抛 `VersionConflictException`。适合「读后改、确保中间无人改过」的场景（典型管理后台编辑表单）。

`VersionConflictException`、配置/校验错误分别有独立异常类型（`com.yomahub.liteflow.publisher.exception.*`），方便上层区分「冲突重试」与「参数错误」。

**生命周期：** `RulePublisher` 实现 `AutoCloseable`。SQL / PostgreSQL 后端每次操作借连接；Redis / MongoDB / zk / etcd / Nacos 可能持有客户端连接，用完必须 `close()`（推荐 try-with-resources）。外部传入的 `DataSource`、`MongoClient`、`ConfigService` 或其他客户端仍归调用方所有，不会被 Publisher 关闭。

**事务/原子性保证：**

- **SQL**：单事务内先锁定 `change_lock.lock_id = 1`，再完成 UPSERT 内容行 + INSERT change_log，回滚一起回滚。
- **PostgreSQL**：单事务内先锁定 `change_lock.lock_id = 1`，再完成 UPSERT 内容行 + INSERT change_log，并用 `RETURNING seq` 返回提交序号。
- **MongoDB**：多文档事务内完成内容 CAS、sequence 自增和 change_log 插入。
- **Redis**：一段 Lua 脚本在 Redis 单线程内原子完成 HSET 内容 → SADD 索引 → INCR seq → ZADD changelog，四步要么全成要么全不成，中间状态不可见。
- **zk**：一个 multi-op 事务内原子写 meta + content znode。
- **etcd**：一个事务（Txn）内原子写 meta + content key。
- **Nacos**：读取当前 Catalog 后以其 MD5 为条件执行 CAS，原子替换正文、业务版本、sequence 和 `lastChange`；并发 CAS 失败会重新读取后重试，最多 8 次。

### 8.2 SQL 兼容门面（不推荐新代码使用）

SQL 模块仍保留 `com.yomahub.liteflow.repository.sql.SqlRulePublisher`。它的无参构造从全局 `LiteflowConfig` 取连接配置，但不支持 route / namespace / expectedVersion，也没有统一 Publisher 的 `change_lock` 发布顺序协议。它不能作为多节点或并发发布场景的生产写入入口；2.16.1 的新代码和管理后台必须使用 [§8.1](#81-推荐统一发布-api) 的 `RulePublisherFactory` + `SqlPublisherConfig`。

下面代码只用于识别和迁移旧调用，不建议新增：

```java
SqlRulePublisher publisher = new SqlRulePublisher();
long v = publisher.publishChain("orderChain", "THEN(a, b)");
publisher.publishScript(scriptRecord);   // 传 ScriptRecord
publisher.removeChain("orderChain");
publisher.removeScript("s1");
```

迁移时把连接参数放入 `SqlPublisherConfig`，并把 `ScriptRecord` 转成 `PublishScriptRequest`。统一 Publisher 会执行完整的事务、顺序锁和版本冲突协议。

### 8.3 停用（enable=0）

v1 的 Publisher **没有** `enableChain/enableScript` API（留作后续）。如需临时停用而不删除，可直写存储把 `enable` 置 0：

- SQL：`UPDATE lf_chain SET enable=0 WHERE application_name=? AND chain_id=?`
- PostgreSQL：`UPDATE lf_chain SET enable=FALSE WHERE application_name=? AND chain_id=?`
- MongoDB：更新对应文档的 `enable=false` 并递增 `version`。
- Redis：`HSET {prefix}:{app}:chain:{id} enable 0`
- zk / etcd：把对应 meta 节点里的 enable 标志置 0（编码见各后端 `*RecordCodec`）。
- Nacos：当前 Catalog 协议不接受 `enable=false` 记录，不支持直改停用；请使用 `removeChain` / `removeScript`。

注意直改 enable 不会产生变更日志/通知，各节点要等**下个对账周期**（默认最多 60s）才感知；zk/etcd 若改了 meta 节点内容会触发 watch，则秒级感知。已在缓存中的编译产物在感知前会继续执行。想立即生效，请用 `removeChain`（删除走变更通知，秒级收敛），或停用后再按 [§8.4](#84-绕过-api-直接写存储的规范不推荐但可做) 规范补一条变更日志。

### 8.4 绕过 API 直接写存储的规范（不推荐，但可做）

如果你已有自己的管理后台、不想引 Java 客户端，也可以直接写存储，但**必须**完整复制 Publisher 的事务/原子语义，缺一步都会导致节点收敛失败。

**SQL 直写规范**（一个事务内完成）：

1. 事务开始后先执行 `SELECT lock_id FROM lf_change_lock WHERE lock_id = 1 FOR UPDATE`，并持锁到提交或回滚。自定义表前缀时同步替换表名；不要按 `application_name` 拆锁。
2. UPSERT `lf_chain` / `lf_script` 行：`version = version + 1`（行锁下原子自增，**不要**先 SELECT 再 Java +1，并发发布会丢更新），重算并写入 `content_md5`（**chain = `MD5(el_data)`，不含 route_data；script = `MD5(script_data)`**，必须与 Publisher 的算法一致，否则会产生虚假对账 diff）。
3. `INSERT INTO lf_change_log (application_name, target_type, target_id, op, version) VALUES (...)`。
4. 提交事务（回滚要一起回滚）。
5. 删除场景：获取同一顺序锁后，DELETE 内容行 + INSERT 一条 `op=DELETE` 的 change_log，仍在同一个事务内完成。

PostgreSQL 直写遵循同一事务协议，同样先 `SELECT ... FROM lf_change_lock ... FOR UPDATE`；MongoDB 直写必须在一个多文档事务内完成正文 CAS、`lf_sequence` 自增和 `lf_change_log` 插入。standalone MongoDB 无法满足该协议。

**Redis 直写规范**：必须用一段 Lua 脚本完成 HSET 内容 → SADD 索引 → INCR seq → ZADD changelog 四步（脚本可参考 [`lua/publish-chain.lua`](../liteflow-rule-db/liteflow-rule-db-redis/src/main/resources/lua/publish-chain.lua)），**不能用普通命令拼**——拼出来在多命令之间存在竞态，可能让别的客户端读到「内容已更新但 seq 没推」的中间态。

**zk / etcd 直写规范**：必须在一个事务（zk multi-op / etcd Txn）内同时写 meta 和 content，保证二者版本一致。zk 不要绕过事务单独改一个节点。

**Nacos 不支持通过控制台直接改 Catalog。** 正确发布需要基于当前内容 MD5 做 CAS，同时维护正文 MD5、业务版本、连续 sequence 和 `lastChange`；控制台覆盖写无法保持这套并发语义。请只使用 `RulePublisher`，需要迁移／恢复时也应由受控工具调用同一 API。

### 8.5 content_md5 对账双保险

对账时先比 `version`，**相同再比 `content_md5`**。除 Nacos 外，比的是 `content_md5` **列／字段的存量值**——引擎不会拉取内容重算哈希（manifest 只查元数据列／字段，不拖全文）。所以这道双保险防的是「version 判据失灵、但指纹仍然可信」的场景：

- 备份恢复 / 跨环境导表：版本号恰好撞车（都是 v7）但内容不同 → md5 不同 → 对账纠正。
- 写方更新了内容和 md5、但 version 没加上去（工具 bug、并发丢更新）→ md5 不同 → 对账纠正。

**它防不住「只改内容、version 和 content_md5 都不动」的裸改**——两个元数据都没变，对账每轮都判定「无变化」，改动**永不生效**。这是手改库最常见的坑。

**手动改一条规则的最小正确姿势**（临时运维、不想走 Publisher 或 §8.4 完整规范时）：

```sql
UPDATE lf_chain
SET el_data     = 'THEN(a, c, b, s1)',
    version     = version + 1,     -- 必须：对账感知变更的主判据
    content_md5 = MD5(el_data)     -- 建议：保持指纹与内容一致（MySQL 的 SET 从左到右求值，取到的是新 el_data）
WHERE application_name = 'your-app' AND chain_id = 'chain1';
```

只做这一步，最迟 `reconcile-seconds`（默认 60s）生效；想在 seq 轮询周期（SQL 默认 3s）内生效，再按 §8.4 补一条 change_log。`lf_script` 同理（指纹是 `MD5(script_data)`）。停用一条 chain 只需 `enable = 0`——它会从 manifest 消失，对账按 DELETE 处理，无需动 version。

这是兜底，**不是**鼓励绕过规范——规范路径才是快路径。

Nacos 是例外：它每次读取都会对 Catalog 内的正文重新计算 MD5 并校验，指纹不匹配会直接拒绝整份 Catalog；仍然不能绕过 Publisher 修改，原因见 §8.4。

### 8.6 发布校验与依赖顺序

Publisher 保证的是**单个目标的存储原子性和版本并发控制**，不负责解析或编译业务规则：

- 它会校验必填字段、长度、后端键名等存储约束，但不会校验 EL 语法，也不会确认 Java 组件、子 chain 或脚本节点已经存在。
- 一次 API 调用只原子发布一个 chain 或 script。当前没有把多条相互依赖规则作为 bundle 同时切换的事务 API；多次调用之间始终存在最终一致性窗口。
- 新增或升级依赖时，按「脚本／叶子子 chain → 引用它们的父 chain」发布；删除时反向操作，先移除父 chain 对依赖的引用，再删除脚本或子 chain。
- 发布前应在隔离环境使用与生产相同的 Java 组件和脚本插件执行冷加载测试。管理后台收到 `PublishResult` 只表示存储写入成功，不表示所有执行节点已经编译成功。
- 回滚应把已验证的旧正文作为**新版本**重新发布，不能把存储中的 `version` 直接改小。已存在成功版本时，新版本加载失败会保留 last-good generation 继续服务，具体状态见 [§12](#12-降级语义)。

---

## 9. 一致性与收敛模型

### 9.1 两条腿

Rule-DB 的多节点收敛靠两条独立的机制叠加，任何一条都能把变更传到所有节点。两条腿的形态随后端而异：

| 后端 | 变更通知腿 | 周期 | 对账腿 |
|---|---|---|---|
| **SQL** | seq 轮询（`SELECT MAX(seq)`） | `poll-seconds`（默认 3s） | 全量对账，`reconcile-seconds`（默认 60s） |
| **PostgreSQL** | seq 轮询（`SELECT MAX(seq)`） | `poll-seconds`（默认 3s） | 同上 |
| **MongoDB** | seq 轮询（sequence + change_log） | `poll-seconds`（默认 3s） | 同上 |
| **Redis** | seq 轮询（`GET seq`） | `poll-seconds`（默认 3s） | 同上 |
| **ZooKeeper** | watch 实时推送（CuratorCache 监听 meta 节点） | 毫秒级 | 同上 |
| **etcd** | watch 实时推送（按 revision 订阅） | 毫秒级 | 同上 |
| **Nacos** | Listener 推送（监听应用 Catalog） | 毫秒级或亚秒级 | 同上 |

zk / etcd / Nacos 使用长连接监听，轮询腿对它们不生效；SQL / PostgreSQL / MongoDB / Redis 没有推送通道，靠 seq 轮询。Nacos Listener 若跳过中间版本，sequence 断档会立即触发一次全量对账。无论哪种，**周期对账都是兜底**——通知／轮询都失效也必收敛。

> Redis 模式**没有 pub/sub 推送**。有些同类设计会用 Redis `PUBLISH`/`SUBSCRIBE` 做毫秒级推送，本实现没有采用——Redis 的变更感知和 SQL 一样靠 seq 轮询。如果你依赖更快的 Redis 收敛，把 `poll-seconds` 调小（代价是更频繁的 `GET seq`）。

### 9.2 收敛窗口

任何变更最迟在 **`max(通知延迟, 对账周期)`** 内被所有节点感知。典型值：

- zk / etcd 模式：watch 毫秒级 + 对账 60s → 最迟 60s 内全集群收敛（实际多数情况毫秒级）。
- Nacos 模式：Listener 推送 + 对账 60s → 最迟 60s 内全集群收敛（实际多数情况为毫秒级或亚秒级）。
- SQL 模式：轮询 3s + 对账 60s → 最迟 60s 内全集群收敛（实际多数情况 3s 内）。
- PostgreSQL / MongoDB 模式：轮询 3s + 对账 60s → 最迟 60s 内全集群收敛（实际多数情况 3s 内）。
- Redis 模式：轮询 3s + 对账 60s → 最迟 60s 内全集群收敛（实际多数情况 3s 内）。

同一条记录从创建到删除期间，业务版本单调递增；变更通知按版本和内容指纹做幂等保护，迟到的旧通知不会把已激活版本打回旧版。**删除后使用相同 id 重建属于新的记录，版本会重新从 1 开始**，不能把它理解为原记录继续递增。备份恢复若会降低业务版本，必须按 [§14.2](#142-备份恢复) 的流程处理。

> **只发脚本、不发 chain 也会收敛。** 脚本新版发布后，**所有**引用该脚本的已编译 chain（含多条 chain 共享同一脚本的场景）都会在收敛窗口内切到新脚本，无需重发 chain。这是脚本级变更的常规姿势。

### 9.3 一致性语义（务必读）

Rule-DB 提供的是**最终一致性、秒级收敛窗口**，**不是**原子切换/线性一致：

- 你发布一个新版本后，在收敛窗口内，**不同节点可能短暂地跑着不同版本**（旧节点还在旧版，收到通知的节点已切新版）。
- 进行中的执行持有旧条件树引用跑完，新执行拿新版（与现有 copy-on-write 语义一致），**不会**所有节点同一逻辑时刻切换。
- 这对绝大多数业务编排场景是足够的（你升级规则时本来就该接受短暂的版本差异）；如果你的业务要求「全集群同一时刻切版」，Rule-DB 当前版本不满足，请别用它。

---

## 10. 内存与性能

### 10.1 内存模型

| 数据 | 位置 | 何时驻留 |
|---|---|---|
| **规则清单、版本戳和状态索引**（`chainId → state`、`nodeId → state + 元数据`） | JVM 常驻 | 整个生命周期；条目数随规则总量线性增长 |
| **影子 `Chain` / `Node` 对象** | JVM 常驻 | 每条启用的清单记录对应一个轻量对象，内容尚未加载时也存在 |
| **EL 文本 + 编译后的条件树** | JVM 有界缓存 | 命中时驻留，Caffeine 按访问热度淘汰后退回影子 |
| **脚本源码 + 编译产物** | JVM 有界缓存 | 同上；通过 chain 的引用计数联动淘汰 |

关键性质是：**正文和编译产物的常驻规模由缓存容量限制，但 JVM 总内存仍与规则总量有关。** 如果库里有 10 万条规则、热点只有 200 条，JVM 不会常驻 10 万条 EL／脚本正文和编译产物，但仍会常驻 10 万条影子对象及其状态索引。大清单上线前必须使用真实规则规模做堆内存和启动 Manifest 基准，不能只按 `cache.capacity` 估算容量。

### 10.2 执行热路径

```
execute2Resp(chainId)
  → FlowBus.getChain(chainId)                      // 本地 map 查找
  → 已编译 → 直接执行                                // 零远程调用
  → 否则（影子 / 收到变更后被失效）→ Chain 上 double-checked locking：
       repository.fetchChain(chainId)              // 一次远程读，带 fetch-retry-times 重试
       → LiteFlowChainELBuilder 构建条件树
       → 写入缓存，登记引用的脚本节点（引用计数 +1）
  → 子链引用（chain 调 chain）递归同一懒加载路径
  → 执行到脚本节点且执行器无产物：
       per-node double-check → fetchScript → loadScript → 缓存产物
```

一致性由**失效驱动**而非读时校验：热路径不逐次比对版本，变更同步（watch/轮询/对账）到达时把对应缓存态置为失效，下次执行走懒加载分支。因此缓存命中的执行路径与原有模式性能基本无差。

### 10.3 调优建议

- **`cache.capacity`**：按你的热点 chain 条数估，默认 500 够大多数应用。设小了频繁淘汰→频繁回源；设大了多吃堆内存。脚本没有独立容量参数——它跟 chain 联动淘汰（chain 被淘汰时，它引用的脚本引用计数减一，归零时一起清）。
- **`cache.preload-chain-ids`**：把首屏/高 QPS 的关键 chain 列在这里，启动时立即拉取编译，抹平冷启动尖刺。非关键链路不必预热，懒加载就够了。
- **路由 chain**：`executeRouteChain` 为取得 route 元数据，会在路由执行前逐个回源并编译所有尚未就绪的 Rule-DB chain，而不是只加载最终匹配的 chain。大清单使用路由模式时，应把第一次路由请求视为批量冷加载，压测其延迟并考虑启动预热或拆分 `application-name`。
- **`sync.poll-seconds`**（SQL / PostgreSQL / MongoDB / Redis）：觉得 3s 不够及时可调小（代价是更频繁的序号查询）；zk / etcd 用 watch，Nacos 用 Listener，此项对它们不生效。
- **`sync.reconcile-seconds`**：60s 是经验值，是「极端兜底」周期，调小意义不大、反而增加全量 diff 开销。
- **Nacos Catalog 体积**：每次冷读取和对账都处理整份 Catalog。通过拆分 `application-name` 控制单应用规则量，并监控配置体积、冷加载耗时和发布冲突率；大 Catalog 不应仅靠增大服务端上限硬撑。

### 10.4 v1 实现注记：惰性刷新与 last-good

驻留条目收到变更通知后会标记为待刷新，但已成功激活的条件树／脚本产物不会立即销毁。下次执行在总线外回源并编译候选版本：成功后原子替换，失败则状态记为 `FAILED`，`desiredVersion` 保持新版而 `activeVersion` 保持旧版，旧的 last-good generation 继续执行。没有任何已激活版本的冷规则加载失败时，执行才会抛出加载异常。进行中的执行始终持有原引用跑完。

因此收到变更后的第一次执行仍可能承担回源和编译延迟，且候选版本损坏时后续执行会继续尝试加载新版；「后台预编译、消弭首个请求延迟」不在当前版本。

---

## 11. 可观测性

Rule-DB 运行时会暴露一个**只读结构快照**（`RuleDbRuntimeSnapshot`），不含规则/脚本全文，用于运维观测同步进度、定位加载失败的目标。

### Spring Boot：actuator 端点

Spring Boot 两个 LiteFlow starter 已传递 `liteflow-metrics`，但 Spring Boot Actuator 在 starter 中是 optional 依赖。应用需要显式引入：

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-actuator</artifactId>
</dependency>
```

并启用、暴露 `liteflow` 端点：

```properties
management.endpoint.liteflow.enabled=true
management.endpoints.web.exposure.include=health,info,liteflow
```

之后才可以访问：

```
GET /actuator/liteflow/ruledb
```

返回 JSON（节选）：

```json
{
  "active": true,
  "provider": "sql",
  "changeSource": {
    "status": "UP",              // STARTING / UP / DEGRADED / DOWN
    "recentError": null,
    "lastSuccessTime": 1720000000000,
    "cursor": 128                // 已应用的最新变更序号
  },
  "lastAppliedSeq": 128,
  "lastSuccessfulReconcileTime": 1720000060000,
  "lastReconcileError": null,
  "targets": {
    "shadow": 2, "ready": 48, "stale": 1,
    "loading": 0, "failed": 1, "deleted": 0
  },
  "failedTargets": [
    { "targetType": "chain", "targetId": "brokenChain",
      "status": "FAILED", "desiredVersion": 5, "activeVersion": 4,
      "error": "fetch chain[brokenChain] failed after 3 retries: ..." }
  ]
}
```

字段含义：

- **`changeSource.status`**：变更通道健康度。`UP`=正常，`DEGRADED`=最近一次轮询/watch 报错但仍在重试，`DOWN`=已关闭，`STARTING`=尚未激活。
- **`targets`**：所有 chain/script 按生命周期状态的计数：
  - `shadow`：已登记索引、内容未加载（冷态）。
  - `loading`：正在回源加载。
  - `ready`：已加载且与权威版本一致（正常态）。
  - `stale`：权威版本变了，本地待刷新到期望版本。
  - `failed`：加载失败（见 `failedTargets` 明细）。
  - `deleted`：已从权威源删除、待清理。
- **`failedTargets`**：处于 `failed` 的目标明细（最多 20 条），含期望版本、当前已激活版本、错误信息。`activeVersion > 0` 表示仍有 last-good generation 服务，`activeVersion = 0` 才表示没有可回退的成功版本。

> 该端点由 `liteflow-metrics` 的 `LiteflowMetaView` 提供，经 `@Endpoint(id="liteflow")` 暴露。同一端点还有 `/actuator/liteflow/chains`、`/actuator/liteflow/nodes` 等结构检视能力（详见 metrics 指南）。

### 非 Spring / 无 actuator 环境

直接调用 `com.yomahub.liteflow.repository.RuleDbRuntime.snapshot()` 拿到同一个 `RuleDbRuntimeSnapshot` 对象，自行序列化或对接你的监控。Solon 环境无自动 actuator，也用此方式。

---

## 12. 降级语义

存储故障时 Rule-DB 的行为有明确边界：

| 故障场景 | 行为 |
|---|---|
| **启动时存储不可用** | `FlowExecutor` 初始化阶段拉取 manifest 失败会**直接抛异常、启动失败**（不会降级为空规则跑起来）。Rule-DB 模式的启动强依赖存储可用——和下面「运行期存储挂了」是两回事。 |
| **运行期存储不可用，已有成功激活版本** | 照常执行 last-good generation。即使已感知到更高的期望版本，只要旧版仍是已激活版本，新版回源失败也不会先销毁旧版；运行时记录 `FAILED` 并在后续执行继续尝试新版。 |
| **运行期存储不可用，没有已激活版本** | fetch 按 `fetch-retry-times`（默认 3）重试，仍失败抛 `ChainLoadException`（区别于 `ChainNotFoundException`——前者是「规则存在但取不回来」，后者是「规则不存在」）。存储恢复后下次执行自动回源，无需干预。 |
| **变更通道故障**（轮询报错 / watch 或 Listener 断线） | 标记 `DEGRADED` 并重试。SQL / PostgreSQL / MongoDB / Redis 轮询失败会在下个周期重试；zk / etcd 断线后重建监听并触发全量对账；Nacos 由客户端维护监听，回调损坏、序号断档或消费失败时请求全量对账。断线窗口由周期对账兜底。 |
| **change_log / changelog 被清理或损坏** | SQL / PostgreSQL 的 `seq` 是全表自增序号，不同 `application_name` 之间跳号正常；水位已前进但读不到本应用记录时会请求对账，其他被裁剪的历史主要由周期对账补齐。MongoDB / Redis 使用连续的应用级序号，可以直接检测断档。Nacos 只保留连续 sequence 和最后一条变更，序号断档或内容损坏会请求全量对账。 |
| **fetch 到 enable=false 或行／节点不存在** | 若已有激活版本，本次普通候选加载失败仍保留 last-good；若无激活版本则抛 `ChainLoadException`。后续对账确认目标已删除后会从索引和 FlowBus 移除，再执行时按 chain 不存在处理。 |
| **变更已感知但回源／编译新版失败** | 若存在已激活版本，记录 `FAILED`，保留旧 `activeVersion` 并继续执行；之后每次执行继续尝试 `desiredVersion`。若从未成功激活过任何版本，则本次执行抛 `ChainLoadException`。chain 和脚本遵循相同语义。删除或删除后重建会改变目标身份，不属于普通升级失败，不能依赖已删除的旧版继续服务。 |
| **SQL 缺表且未开 `auto-init-table`** | 首次访问存储时报 `ConfigErrorException`，错误信息内含完整可复制执行的 DDL。 |

一句话：**已成功激活的 last-good generation 是普通升级失败时的可用性下限**；冷规则首次加载、显式删除、删除后重建和缓存淘汰不应被理解为永久保留旧版。

---

## 13. 限制与已知边界

为避免误用，下面这些 v1 的限制请务必先看一眼：

1. **与 `rule-source` 互斥。** 同时配置 `liteflow.rule-source` 和 `liteflow.rule-db.*` 会启动直接报错。Rule-DB 和老插件模式不能混用。

2. **七个 Rule-DB 插件同一时刻 classpath 只能有一个。** `liteflow-rule-db-sql` / `-postgresql` / `-mongodb` / `-redis` / `-zk` / `-etcd` / `-nacos` 七选一。同时存在多个会启动报错要求只保留一个。

3. **Redis Cluster 必须配置 `key-hash-tag`。** `RedisRulePublisher` 的 Lua 脚本会触碰 4 个键（`chain:{id}`、`chain-ids`、`seq`、`changelog`），多键 `EVAL` 要求这些键落在同一 slot，否则 Cluster 会以 `CROSSSLOT` 错误失败。模块已通过 hash-tag 支持：配置 `liteflow.rule-db.redis.key-hash-tag` 后，所有键按 `{prefix}:{hashTag}:{app}:...` 布局、固定到同一 slot，原子发布可正常工作。
   - **cluster 模式（多地址且无 `master-name`）**：`key-hash-tag` 为必填，未配置时建连直接抛 `ConfigErrorException`（fail-fast，不会带病启动）。
   - **单机 / 哨兵模式**：无 slot 约束，`key-hash-tag` 可不配；配置了也会生效（改变键布局，请勿在已有数据的实例上随意增删该配置）。

4. **MongoDB 必须支持多文档事务。** 运行时的 Manifest 快照读取与 Publisher 都使用事务，因此只支持副本集或分片集群；standalone MongoDB 不受支持。

5. **Nacos 需要 Server 2.x，并受单配置容量约束。** 1.x 不提供本模块使用的 CAS 发布能力。一个 applicationName 的全部正文、元数据和最后变更记录位于同一 Catalog；实际可用上限同时受 Nacos 服务端 `nacos.core.config.max-size`、数据库字段和代理／网关限制。模块不自动分片，接近上限会导致发布失败；上线前必须按生产链路验证最大 Catalog，并为 JSON 与后续规则增长留足余量。

6. **一致性语义是最终收敛、秒级窗口，不是原子切换/线性一致。** 见 [§9.3](#93-一致性语义务必读)。要求全集群同一逻辑时刻切版的场景，当前版本不满足。

7. **v1 不提供的实现（SPI 已就位、留作后续）：**
   - Apollo 的 Rule-DB 实现。`RuleRepository` SPI 在 core 里已经定义好，后续可按同一套契约扩展。
   - `enableChain/enableScript` API（停用目前靠直写存储，见 [§8.3](#83-停用enable0)）。
   - 节点实例 ID 持久化（旧 sql 插件的 `NodeInstanceIdManageSpi` 能力）。
   - 管理 UI / 控制台。v1 只提供 Publisher API 与写入规范。

8. **并发发布语义。** 不传 `expectedVersion` 时是无条件 UPSERT；传 `expectedVersion=0` 表示“仅当不存在时创建”；传正数表示按版本做 CAS 更新。七个后端都保证同一存续记录的成功发布版本单调递增；删除后用相同 id 重建时版本从 1 开始。

9. **应用元数据与 Rule-DB id 冲突会启动失败。** 通过 `LiteFlowChainELBuilder` 手动 build、且 id 不在存储清单中的 chain 可以共存；但手写 chain 与存储 chain 同 id，或应用注册的 script node 与存储 script 同 id 时，Rule-DB 初始化会抛 `ConfigErrorException`，不会覆盖应用对象。请保证两边 id 集合不相交。

### 13.1 发布参数与后端限制矩阵

统一请求对象会校验必填字段；各后端还会按照自身表结构或键布局做额外校验。下面列出 2.16.1 代码中最容易踩到的硬边界，长度均按 Unicode code point 计，只有 SQL 正文限制明确按 UTF-8 字节数计：

| 后端 | id／字段限制 | 正文与键限制 |
|---|---|---|
| **SQL（MySQL DDL）** | `application-name` 64；chain/node id 128；namespace 64；脚本名 128；type/language 32；`table-prefix` 最长 54 且只能用 ASCII 字母、数字、下划线 | `el`、`route`、`script` 分别不得超过 65,535 UTF-8 bytes，对齐 `TEXT` |
| **PostgreSQL** | `application-name` 64；chain/node id 128；namespace 64；脚本名 128；type/language 32；`table-prefix` 最长 52 且只能用 ASCII 字母、数字、下划线 | 正文使用 PostgreSQL `TEXT`；仍受数据库、驱动和运维设置限制 |
| **MongoDB** | `application-name` 和 id 128；namespace 128；脚本名 256；type/language 64；database 只允许 ASCII 字母、数字、下划线、连字符；collection prefix 最长 64 | 单文档与事务大小受 MongoDB 服务端限制 |
| **Redis** | id 最长 128，且不能包含 `:` 或空白；namespace 64；脚本名 128；type/language 32 | Cluster 必须配置 `key-hash-tag`；正文还受 Redis 单值、Lua 和客户端限制 |
| **ZooKeeper** | applicationName、rootPath 每个 segment 不能包含 `/`、`..` 或控制字符；rule id 不能为空或包含 `/` | 单个编码后的 meta 或 content znode 上限为 960 KiB |
| **etcd** | rule id 不能为空或包含 `/`；applicationName 和 rootPath 会直接参与 key 前缀 | 受 etcd 请求大小、配额和历史压缩设置限制 |
| **Nacos** | data-id-prefix、application-name、group 只能包含字母、数字、`_`、`-`、`.`、`:` | 一个应用的全部规则位于单个 Catalog，受 Nacos 单配置容量限制 |

跨后端迁移时应按**目标后端中更严格的限制**提前校验，不能假设在 MongoDB 或 PostgreSQL 可写入的 id／正文一定能原样迁移到 Redis、ZooKeeper 或 SQL。

---

## 14. 迁移、备份与权限

### 14.1 从旧规则插件迁移

2.16.1 不提供把 `liteflow-rule-*` 数据自动转换为 Rule-DB 存储的迁移器。推荐用受控迁移程序读取旧数据，再调用目标后端的统一 `RulePublisher`，不要直接拼接 Rule-DB 表、键或 Catalog：

1. 确定唯一的 `application-name`，在生产等价环境创建目标存储结构，并用 Publisher 完成必要的表、索引或路径初始化。
2. 冻结旧管理后台的写入，记录迁移基线；先发布脚本和叶子子 chain，再发布引用它们的父 chain。迁移程序使用 `expectedVersion=0`，遇到重复 id 时停止处理，而不是静默覆盖。
3. 对比 chain／script 数量、id、正文 MD5、namespace 和 route；使用生产相同的 Java 组件、脚本插件及配置执行冷启动和关键 chain 回归测试。
4. 切换执行应用时移除旧 `liteflow-rule-*` 插件和 `liteflow.rule-source`，classpath 只保留一个 Rule-DB 后端。不要让新旧插件在同一 `FlowExecutor` 中双读。
5. 保留旧存储只读快照直到观察期结束。需要回滚时回退应用依赖和配置到旧插件，不要在同一应用内临时混用两套权威源。

迁移期间若业务仍需修改规则，应在切换前再次冻结并重做增量，或由上层管理系统实现经过验证的双写；Rule-DB 本身不提供跨旧插件的双写事务。

### 14.2 备份恢复

- 备份必须覆盖同一后端的完整协议状态：SQL / PostgreSQL 的四张表（包括 `change_log` 和 `change_lock`）、MongoDB 的四个 Collection、Redis 的内容键／id 集合／seq／changelog、ZooKeeper / etcd 的 meta 与 content，以及 Nacos 的整份 Catalog。只恢复正文、不恢复版本和序号会破坏收敛判据。
- 不要把更低 `version` 的备份在线覆盖到仍在运行的相同 `application-name`。节点可能把它当成迟到旧版本而忽略。完整灾备恢复应停止该应用的 Publisher 和执行节点，原子恢复协议状态后再重启；另一种做法是恢复到新的 `application-name` 后切流。
- 在线回滚单条规则时，应通过 Publisher 把旧正文发布成更高的新版本。不要直接降低 `version`，也不要只改正文而不更新 MD5 和变更日志。
- SQL / PostgreSQL 恢复后必须确认 `change_lock` 中仍存在 `lock_id = 1`；Redis / MongoDB 要确认 sequence 不低于保留的 changelog；Nacos Catalog 必须整体恢复并通过正文 MD5、sequence 和 `lastChange` 校验。
- 恢复后先启动一个执行节点，检查 Rule-DB 快照、冷加载关键 chain 并观察至少一个 `reconcile-seconds` 周期，再逐步恢复流量。

### 14.3 执行账号与发布账号

建议分离只读执行账号和可写 Publisher 账号。精确 ACL 语法随后端和部署方式而异，但能力边界如下：

| 后端 | 执行账号 | Publisher 账号 |
|---|---|---|
| **SQL / PostgreSQL** | 对规则表、日志表和锁表 `SELECT`；若开启 `auto-init-table` 还需要 DDL 权限 | `SELECT`、`INSERT`、`UPDATE`、`DELETE`，以及对 `change_lock` 的行锁权限；初始化时需要建表权限 |
| **MongoDB** | 读取四个 Collection，并允许事务／快照会话 | 读写四个 Collection、执行事务；首次初始化还需创建 Collection／索引 |
| **Redis** | 读取内容、id 集合、seq 和 changelog 所需命令 | 在相同 key 前缀上执行发布 Lua 及其读写命令 |
| **ZooKeeper** | 四棵 meta/content 路径及子节点的递归读取和 watch | 创建、读取、更新、删除这些路径并执行 multi-op |
| **etcd** | 规则前缀的 Range 和 Watch | 同一前缀的 Range、Put、Delete 和 Txn |
| **Nacos** | 对 Catalog 的读取和 Listener 权限 | 读取 Catalog，并使用 CAS 发布配置的权限 |

执行侧若配置为自动建表／自动初始化，就不再是严格只读账号。生产环境若要求最小权限，应先用 Publisher 或 DBA 初始化结构，再关闭自动初始化并使用只读执行账号。
