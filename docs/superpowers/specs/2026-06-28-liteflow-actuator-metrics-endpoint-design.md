# LiteFlow Actuator 指标端点设计

- 日期：2026-06-28
- 状态：设计已确认，待 review
- 涉及模块：`liteflow-core`、新增 `liteflow-metrics`、`liteflow-spring-boot-starter`、`liteflow-spring-boot4-starter`

## 1. 背景与目标

希望给 LiteFlow 增加一套可观测能力，让使用方能：

1. 查看所有**已注册的 chain 和 node**及其定义（结构检视）；
2. 获取每个 chain / node 的**运行指标**（执行次数、耗时、成功/失败、错误率等），并对接主流监控体系。

经讨论确定的方向：

- **指标以对接监控系统为主**：核心指标注册成 Micrometer Meter，由 Spring Boot Actuator 自动暴露到 `/actuator/prometheus`、`/actuator/metrics`，交给 Prometheus / Grafana 做大盘与告警。LiteFlow 自身不存历史数据、不自建采集存储。
- **采集粒度为 chain + node 两级**：分别按 `chainId` 和 `nodeId` 聚合，基数 = chain 数 + node 数，可控；既能定位"哪条链慢"也能定位"哪个组件慢"。
- **额外提供一个只读结构端点** `/actuator/liteflow`：补齐 Micrometer 看不到的定义信息（EL 原文、组件 class/type、未执行过的 chain/node 等）。

### 非目标（Non-goals）

- 不做指标的持久化、历史回溯、聚合存储（交给 Prometheus）。
- 不替换或改造现有 `MonitorBus`（日志型、受 `monitor.enable-log` 守护），新系统与其完全独立、互不影响。
- 不为"让未执行过的 chain/node 也出现在 Prometheus"而预注册全部 Timer（会产生大量 0 值空序列）。这一需求由结构端点满足，注册总量用 Gauge 体现。
- Phase 1 不覆盖 Solon（Solon 无 Spring Actuator）；但 `liteflow-metrics` 模块本身框架无关，Solon 后续可单独接入 MeterRegistry，列为后续工作。

## 2. 现状盘点（设计依据）

- **结构数据现成**：`FlowBus` 持有静态 `chainMap` / `nodeMap`，`getChainMap()` / `getNodeMap()` 直接可取。
  - `Chain`：`chainId`、`el`、`routeEl`、`namespace`（默认 `DEFAULT_NAMESPACE`）、`elMd5`、`routeItem`、`conditionList`。
  - `Node`：`id`、`name`、`type`(`NodeTypeEnum`)、`clazz`、`instance`、`script`、`language`。
  - `LiteflowMetaOperator.getChainsContainsNodeId(nodeId)` → `List<Chain>`，可反查"哪些 chain 包含某 node"。
- **现有监控很薄**：`MonitorBus` + `CompStatistics` 仅按"组件 class 简单名"采集最近 N 条耗时（`BoundedPriorityBlockingQueue`），只往日志打印，无查询接口，无 chain 维度，无成功/失败/错误率。保留不动。
- **埋点机制约束（关键）**：
  - `CmpAroundAspect`（SPI）与 `ICmpAroundAspect`（Spring bean）**都是单实例**——`CmpAroundAspectHolder` 取 `ServiceLoader` 排序后的 `list.get(0)`；`SpringCmpAroundAspectHolder.init()` 直接覆盖。用它们埋点会与用户自己的全局切面互相覆盖，**不可用于本特性**。
  - `LifeCycleHolder` 持有的各 `PostProcess*LifeCycle` 是 **list 形**（可叠加，无冲突）。`Chain.execute()` 用 `try/catch/finally` 包裹，异常时 `slot.setException(e)` 后重抛，`postProcessAfterChainExecute(chainId, slot)` 在 `finally` 中**必定触发**；`ChainEndException` 不进 `slot.exception`（属正常结束）。
  - 生命周期实现的注册：`LifeCycleBeanProcess` 扫描 `instanceof LifeCycle` 的 Spring bean → `LifeCycleHolder.addLifeCycle()`。`NodeComponent.execute()` 的 finally 块中已具备 `nodeId`、`chainId`、`timeSpent`、成败、异常。
  - `DataBus.OCCUPY_COUNT`（`AtomicInteger`）= 当前在用 slot 数。

## 3. 总体架构

```
                         ┌─────────────────────────────────────────┐
                         │ liteflow-core                            │
  Chain.execute() ──────▶│ PostProcessChainExecuteLifeCycle (已有)  │
  NodeComponent.execute()│ PostProcessNodeExecuteLifeCycle (新增)   │
                  ──────▶│ LifeCycleHolder (加一个 list + 分支)      │
                         └───────────────┬─────────────────────────┘
                                         │ 实现 + 订阅
                         ┌───────────────▼─────────────────────────┐
                         │ liteflow-metrics (框架无关)              │
                         │ - ChainMetricsLifeCycle  (chain 指标)    │
                         │ - NodeMetricsLifeCycle   (node 指标)     │
                         │ - LiteflowMeterBinder    (注册表 Gauge)  │
                         │ - LiteflowMetaView       (端点取数 POJO) │
                         │   依赖: liteflow-core + micrometer-core  │
                         └───────────────┬─────────────────────────┘
                                         │ 注入 MeterRegistry / 暴露 @Endpoint
              ┌──────────────────────────┴───────────────────────────┐
   ┌──────────▼───────────┐                            ┌──────────────▼─────────┐
   │ spring-boot-starter   │                            │ spring-boot4-starter   │
   │ (Boot 2/3)            │                            │ (Boot 4)               │
   │ - LiteflowMetricsAuto │                            │ - LiteflowMetricsAuto  │
   │ - LiteflowEndpoint    │                            │ - LiteflowEndpoint     │
   └───────────────────────┘                            └────────────────────────┘
```

### 3.1 模块布局

新增 **`liteflow-metrics`** 模块（框架无关）：

- 依赖：`liteflow-core`、`micrometer-core`（`optional`/`provided`，由宿主提供具体 registry）。
- 内含：
  - `ChainMetricsLifeCycle implements PostProcessChainExecuteLifeCycle`
  - `NodeMetricsLifeCycle implements PostProcessNodeExecuteLifeCycle`
  - `LiteflowMeterBinder implements MeterBinder`（注册表 / slot 池 Gauge）
  - `LiteflowMetaView`：纯 POJO，读 `FlowBus` + 可选读 `MeterRegistry`，为端点提供结构 + 指标快照数据（返回普通 DTO/Map）。
- 该模块只构建于 JDK 8+ 主集（与 core 同级，进 `compile-8-to-16` profile 的模块列表）。

在 **两个 spring-boot starter** 中各加薄壳（逻辑都委托给 `liteflow-metrics`，避免重复）：

- `LiteflowMetricsAutoConfiguration`：把 Spring 托管的 `MeterRegistry` 注入到三个 lifecycle/binder，并注册 `LiteflowMetaView`。守护条件见 §6。
- `LiteflowEndpoint`：`@Endpoint(id = "liteflow")`，`@ReadOperation` 委托 `LiteflowMetaView`。

> Boot 2/3 与 Boot 4 的 Actuator `@Endpoint`/`@ReadOperation`/`@Selector` API 一致，两份壳代码近乎相同，只是放在各自包下、跟随各自的 AutoConfiguration 约定（Boot4 用 `@AutoConfiguration`）。

## 4. core 改动（约 15 行，对称于 chain）

新增节点执行生命周期扩展点，使节点级埋点可叠加、零冲突。

### 4.1 新接口

```java
package com.yomahub.liteflow.lifecycle;

import com.yomahub.liteflow.core.NodeComponent;

/** 节点执行前后的生命周期接口，自 2.x */
public interface PostProcessNodeExecuteLifeCycle extends LifeCycle {

    void postProcessBeforeNodeExecute(NodeComponent cmp);

    /**
     * @param cmp       执行完成的组件（含 nodeId / chainId / type）
     * @param timeSpent 本次执行耗时（毫秒）
     * @param e         执行异常，成功时为 null
     */
    void postProcessAfterNodeExecute(NodeComponent cmp, long timeSpent, Exception e);
}
```

> 说明：`after` 直接带上 `timeSpent` 与 `exception`，因为 `NodeComponent.execute()` 的 finally 里这两者已现成，无需订阅方再自行计时；这样节点指标连"in-flight active"以外的全部信息一次拿全。`before` 仅供需要 active/LongTaskTimer 的订阅方使用。

### 4.2 `LifeCycleHolder` 增补

- 新增静态 `List<PostProcessNodeExecuteLifeCycle> POST_PROCESS_NODE_EXECUTE_LIFE_CYCLE_LIST`；
- `addLifeCycle()` 的 if-else 链补一个 `PostProcessNodeExecuteLifeCycle` 分支；
- 增加 `getPostProcessNodeExecuteLifeCycleList()`；
- `clean()` 中一并清理。

### 4.3 `NodeComponent.execute()` 接入

在现有 finally 块中（已有 `timeSpent`、`cmpStep.isSuccess()`、捕获到的异常、`this` 即 NodeComponent），在 `monitorBus` 统计之后追加：

```java
// 节点执行生命周期（list 形，可叠加，与 monitorBus 独立）
List<PostProcessNodeExecuteLifeCycle> list = LifeCycleHolder.getPostProcessNodeExecuteLifeCycleList();
if (CollUtil.isNotEmpty(list)) {
    list.forEach(lc -> lc.postProcessAfterNodeExecute(self, timeSpent, nodeExecuteException));
}
```

并在 try 起始处（`beforeProcess` 前）对称调用 `postProcessBeforeNodeExecute(self)`。`nodeExecuteException` 即 catch 块捕获到、最终重抛的异常引用（成功为 null）。

> 兼容性：纯增量，默认列表为空时零开销；不改变任何既有行为与 `MonitorBus` 路径。

## 5. 指标目录

命名前缀 `liteflow.`，所有 tag 保持低基数。分位、直方图等不在此处配置，交给标准的 `management.metrics.distribution.*`（见 §6）。

### 5.1 Chain 级（钩子：`PostProcessChainExecuteLifeCycle`，按 `chainId`）

| 指标 | 类型 | Tags | 含义 |
|---|---|---|---|
| `liteflow.chain.executions` | Timer | `chain`, `status`(success/failed) | 执行次数、总/平均/最大耗时、可选分位、成功数、失败数、错误率 |
| `liteflow.chain.active` | LongTaskTimer | `chain` | 当前在途执行数 + 最长在途耗时（发现卡住/堆积） |
| `liteflow.chain.errors` | Counter | `chain`, `exception`(异常类 simpleName) | 按异常类型分布的失败数 |

实现要点：
- `before` 时在**每线程的栈**中压入 `Timer.Sample`（或开始纳秒）与 `chainId`，`after` 时出栈停止——以正确支持同线程嵌套子链（before A → before B → after B → after A）；WHEN 并行子链在各自线程，互不影响。
- `status` 由 `slot.getException()` 是否为 null 判定；`exception` tag 取该异常 `getClass().getSimpleName()`。
- `active` 用 `LongTaskTimer`：`before` 调 `start()` 得到 sample，存栈，`after` `stop()`。

### 5.2 Node 级（钩子：`PostProcessNodeExecuteLifeCycle`，按 `nodeId`）

| 指标 | 类型 | Tags | 含义 |
|---|---|---|---|
| `liteflow.node.executions` | Timer | `node`, `type`(节点类型), `status` | 组件执行次数、耗时分布、成功/失败 |
| `liteflow.node.active` | LongTaskTimer | `node` | 在途组件数（定位慢/挂起组件） |
| `liteflow.node.errors` | Counter | `node`, `exception` | 组件按异常类型的失败数 |

实现要点：
- `after` 直接拿到 `timeSpent` 与 `exception`，记录 Timer（带 `status`）与（失败时）errors Counter。
- `type` 取 `cmp.getType().name()`（或对应枚举名）。
- node 级 active 由 `before`/`after` 的 LongTaskTimer 维护。

### 5.3 全局 / 注册表 Gauge（`LiteflowMeterBinder` 一次性绑定）

| 指标 | 来源 | 含义 |
|---|---|---|
| `liteflow.chains.registered` | `FlowBus.getChainMap().size()` | 已注册 chain 总数 |
| `liteflow.nodes.registered` | `FlowBus.getNodeMap().size()` | 已注册 node 总数 |
| `liteflow.slot.size` | `LiteflowConfig.slotSize` | slot 池容量 |
| `liteflow.slot.occupied` | `DataBus.OCCUPY_COUNT` | 在用 slot 数（关键饱和度，逼近容量即并发吃紧/泄漏） |

### 5.4 指标计算职责归属（谁算 QPS / 平均 / 分位）

LiteFlow 核心代码**不做任何统计计算**，只上报原始测量值，聚合与推导全部交给 Micrometer 与监控后端：

| 层 | 职责 | 由谁承担 |
|---|---|---|
| LiteFlow 记录 | 每次执行结束 `timer.record(耗时)`；出错 `counter.increment()` | 本模块的 lifecycle 钩子，只记原始值 |
| Micrometer 聚合 | 内存中维护 `count`、`totalTime`(sum)、`max`（`LongAdder`） | Micrometer `Timer` |
| Prometheus/Grafana 推导 | QPS、平均耗时、错误率在**查询时**算出 | 监控后端 |

- **QPS**：不落库，由 `rate(count[1m])` 查询时对只增 count 求斜率得到。
- **平均耗时**：不落库，由 `rate(sum[1m]) / rate(count[1m])` 当场相除。
- **P95/P99**：count+sum 无法倒推，需 Micrometer 进程内直方图（开启分位时）或发布 histogram bucket 交 Prometheus `histogram_quantile()`——仍是 Micrometer/Prometheus 机制，非 LiteFlow。

这是与现有 `MonitorBus`（自存最近 N 条、自算平均）的本质区别：本模块**零统计计算、零历史存储**。

### 5.5 Phase 2（可选，本次不实现）

- `liteflow.node.retry`、`liteflow.node.rollback`（需 retry/rollback 钩子）。
- 用 Micrometer `ExecutorServiceMetrics` 绑定 WHEN 并行线程池（活跃线程 / 队列深度）。
- Solon 接入。

## 6. 配置（最小面）

**加入 `liteflow-metrics` 依赖本身即为开关**：没加则零行为；加了即说明要指标。因此 LiteFlow 自有配置仅 1 个可选属性：

```properties
# 可选。检测到 MeterRegistry bean 即自动装配，默认开启。仅在想临时关闭时设 false。
liteflow.metrics.enabled=false
```

装配守护条件：`@ConditionalOnClass(MeterRegistry.class)` + `@ConditionalOnBean(MeterRegistry.class)` + `@ConditionalOnProperty(prefix="liteflow.metrics", name="enabled", matchIfMissing=true)`。

其余可观测细节一律复用 Spring Boot / Micrometer 标准配置，**不另立 LiteFlow 配置**：

- 分位：`management.metrics.distribution.percentiles[liteflow.chain.executions]=0.95,0.99`
- 直方图：`management.metrics.distribution.percentiles-histogram[liteflow.node.executions]=true`
- 过滤/裁剪指标：用户自定义 `MeterFilter` bean。
- 端点暴露：`management.endpoints.web.exposure.include=liteflow,prometheus`（用户标准 actuator 设置，非本特性配置，仅文档提示）。

## 7. 结构端点 `/actuator/liteflow`

`@Endpoint(id = "liteflow")`，全部 `@ReadOperation`，数据由 `LiteflowMetaView` 提供（结构读 `FlowBus`，指标快照可选读 `MeterRegistry`）。

| 路由 | 返回 |
|---|---|
| `GET /actuator/liteflow` | 概览：LiteFlow 版本、parseMode、chains/nodes 注册数、slot size/occupied、chainId 列表、nodeId 列表 |
| `GET /actuator/liteflow/chains` | 全部 chain：chainId、namespace、EL 原文、elMd5、引用的 nodeId 列表 |
| `GET /actuator/liteflow/chains/{chainId}` | 单链详情 + 指标快照（从 MeterRegistry 读 count / mean / max / 错误率，若有） |
| `GET /actuator/liteflow/nodes` | 全部 node：nodeId、name、type、clazz、isScript、language |
| `GET /actuator/liteflow/nodes/{nodeId}` | 单组件详情 + 指标快照 + 包含它的 chain 列表（`LiteflowMetaOperator.getChainsContainsNodeId`） |

- `{chainId}` / `{nodeId}` 用 `@Selector`。
- 指标快照为"尽力而为"：`liteflow.metrics.enabled=false` 或无 MeterRegistry 时，端点仍返回结构信息，指标字段为空/省略。

## 8. 性能影响与基数控制

### 8.1 单次开销

- **未引入 `liteflow-metrics` 时：接近零。** chain 钩子调用在 `Chain.execute()` 中本就存在；node 新钩子在 `NodeComponent.execute()` 的 finally 中仅多一次 `CollUtil.isNotEmpty(空list)` 判断，不分配、不遍历。
- **引入后：亚微秒级，相对组件执行可忽略（通常 <1%）。** 每次执行新增：
  - `timer.record(...)`：一次 Meter 查找（`ConcurrentHashMap` 按 name+tags 取已缓存 Timer）+ 几次 `LongAdder.add`，量级约几十~一两百纳秒；
  - 一个 `Timer.Sample` 小对象 + 每线程栈（支持嵌套链）的一次入/出栈；
  - 出错时一次 Counter 自增。
- 对比基线：`NodeComponent.execute()` 每次本就创建 `CmpStep`、两个 `Date`、`StopWatch` 起停，并打两次 `LOG.info`（start / finished）。**单次日志成本已远高于一次 `timer.record`**；新增开销被既有逻辑与用户业务逻辑（常含 DB/IO）盖过。

### 8.2 基数控制（真正的风险点）

单次耗时不是瓶颈，时间序列基数才是：

- 每个 `(chain,status)` / `(node,type,status)` 组合驻留一个小 Meter（几百字节）并导出一条时间序列。
- 选定的 **chain+node 两级** → 内存 ≈ O(chain 数 + node 数)，几千个量级仅几 MB。
- 已规避的高基数陷阱：被否决的"node-in-chain 三级"（chain×node 可能爆炸）；高基数 tag（requestId、完整异常 message）——`exception` tag 仅取类 simpleName，基数有界。
- 超高吞吐场景下若需进一步压成本，`active`（LongTaskTimer）是最可做成可选 / 移除的一项。

## 9. 兼容性

- **Boot 2/3 与 Boot 4**：Actuator 端点 API 与 Micrometer API 在两者一致；两份薄壳分别置于 `liteflow-spring-boot-starter` 与 `liteflow-spring-boot4-starter`。`liteflow-spring-boot4-starter` 仅在 JDK 17+ 的 `compile-17+` profile 下构建——与现状一致。
- **MonitorBus**：不改动，独立运行。
- **JDK**：`liteflow-metrics` 随 core 进 JDK 8+ 主集。
- **无 Micrometer 环境**：不引入 `liteflow-metrics` 依赖即可，core 改动在列表为空时零开销。

## 10. 测试方案

- **core**：新增 `PostProcessNodeExecuteLifeCycle` 的注册与触发测试（成功 / 失败 / 嵌套链场景下 before/after 调用次数与参数），放在现有 lifecycle 测试模块（参考 `liteflow-testcase-el-springboot` / `-springboot4` 的 `lifecycle` 包）。
- **liteflow-metrics**：用 `SimpleMeterRegistry` 断言执行若干 chain/node 后各 Meter 的 count、tag、status、错误率符合预期；断言注册表 Gauge 数值。
- **starter**：`@SpringBootTest` + Actuator 测试，断言 `/actuator/liteflow` 各路由返回结构正确，且 `/actuator/metrics/liteflow.chain.executions` 可见。Boot2/3 与 Boot4 各一套。
- 沿用项目测试范式：`flowExecutor.execute2Resp("chainId", arg)` 驱动后校验指标。

## 11. 落地顺序（建议）

1. core：新增 `PostProcessNodeExecuteLifeCycle` + `LifeCycleHolder` 增补 + `NodeComponent.execute()` 接入（含测试）。
2. 新建 `liteflow-metrics` 模块：三个 lifecycle/binder + `LiteflowMetaView`（含 `SimpleMeterRegistry` 单测）。
3. `liteflow-spring-boot-starter`（Boot2/3）：AutoConfiguration + `@Endpoint`（含 Actuator 测试）。
4. `liteflow-spring-boot4-starter`（Boot4）：同上薄壳。
5. 文档：在 `docs/` 增补使用说明（依赖引入、暴露端点、Grafana 接入示例）。
