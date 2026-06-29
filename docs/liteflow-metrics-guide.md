# LiteFlow Actuator 指标使用指南

本文介绍如何使用 LiteFlow 的可观测能力：通过 Micrometer 上报 chain / node 的执行指标，并由 Spring Boot Actuator 暴露给 Prometheus 等监控系统，同时提供一个只读的结构检视端点 `/actuator/liteflow`。

读完本文后，你应该能够：

- 引入依赖，让指标与端点自动装配；
- 通过开关与标准的 Actuator / Micrometer 配置控制暴露范围与分位精度；
- 在 Prometheus / Grafana 中查询 QPS、平均耗时、错误率、slot 饱和度；
- 理解 LiteFlow 与 Micrometer / Prometheus 之间的职责边界，以及性能与基数特点。

> 当前仓库根版本：`2.16.0`。
>
> 指标特性由框架无关的 `liteflow-metrics` 模块提供。该模块已经是 `liteflow-spring-boot-starter`（Boot 2/3）与 `liteflow-spring-boot4-starter`（Boot 4）的传递依赖，使用这两个 starter 的项目**无需单独引入 `liteflow-metrics`**。

---

## 1. 引入依赖

指标采集依赖一个 `MeterRegistry`（由具体监控系统提供）。因此除了 LiteFlow starter，还需要引入 `spring-boot-starter-actuator` 和一个具体的 registry（如 `micrometer-registry-prometheus`）。

> `liteflow-metrics` 把 `micrometer-core`、`spring-boot-actuator*` 都标记为 `optional`，不污染你的依赖树；你引入哪个 registry，LiteFlow 就把指标写到哪个 registry。

### Spring Boot 2 / 3 项目

```xml
<dependencies>
    <!-- LiteFlow starter（已包含 liteflow-metrics） -->
    <dependency>
        <groupId>com.yomahub</groupId>
        <artifactId>liteflow-spring-boot-starter</artifactId>
        <version>2.16.0</version>
    </dependency>

    <!-- Actuator：暴露 /actuator/* 端点 -->
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-actuator</artifactId>
    </dependency>

    <!-- 一个具体的 registry：这里以 Prometheus 为例 -->
    <dependency>
        <groupId>io.micrometer</groupId>
        <artifactId>micrometer-registry-prometheus</artifactId>
    </dependency>
</dependencies>
```

Gradle:

```groovy
implementation 'com.yomahub:liteflow-spring-boot-starter:2.16.0'
implementation 'org.springframework.boot:spring-boot-starter-actuator'
implementation 'io.micrometer:micrometer-registry-prometheus'
```

### Spring Boot 4 项目（JDK 17+）

把 starter 换成 `liteflow-spring-boot4-starter` 即可，其余依赖完全一致：

```xml
<dependency>
    <groupId>com.yomahub</groupId>
    <artifactId>liteflow-spring-boot4-starter</artifactId>
    <version>2.16.0</version>
</dependency>
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-actuator</artifactId>
</dependency>
<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-registry-prometheus</artifactId>
</dependency>
```

### 非 Spring / Solon 项目

`liteflow-metrics` 本身框架无关：你只需要显式引入 `liteflow-metrics`，自己构造 `ChainMetricsLifeCycle` / `NodeMetricsLifeCycle` / `LiteflowMeterBinder` 并传入你自己的 `MeterRegistry`。结构端点 `/actuator/liteflow` 是 Spring Actuator 的能力，在非 Spring 环境不可用，但指标依然会写入你的 registry。

---

## 2. 开关与端点暴露

### LiteFlow 自有开关（仅一个）

```properties
# 可选。默认开启（matchIfMissing=true）。
# 仅在需要临时关闭指标采集时显式设为 false。
liteflow.metrics.enabled=true
```

装配守护条件是：类路径存在 `io.micrometer.core.instrument.MeterRegistry` + 容器中存在 `MeterRegistry` Bean + `liteflow.metrics.enabled` 非 false。三者同时满足时，`ChainMetricsLifeCycle`、`NodeMetricsLifeCycle`、`LiteflowMeterBinder` 才会被装配。换句话说，**没有引入任何 registry 就不会有任何指标行为**。

### 暴露 Actuator 端点

端点暴露属于标准的 Spring Boot Actuator 配置，LiteFlow 不另立配置项：

```properties
# 暴露 liteflow 结构端点 + Prometheus 抓取端点 + 通用 metrics 端点
management.endpoints.web.exposure.include=liteflow,prometheus,metrics
# 显式启用 liteflow 端点
management.endpoint.liteflow.enabled=true
```

加上这两行后，`/actuator/liteflow`、`/actuator/prometheus`、`/actuator/metrics` 即可访问。

---

## 3. 指标目录

所有指标命名前缀为 `liteflow.`，tag 全部为低基数（`chainId` / `nodeId` / 状态 / 异常类 simpleName），不包含 requestId、完整异常 message 等高基数维度。

> Prometheus 抓取时会做命名转换：`.` → `_`；Timer 的基础单位是秒，会带 `_seconds` 后缀；Counter 会带 `_total` 后缀。详见第 6 节的 PromQL。

### 3.1 Chain 级指标（按 `chainId` 聚合）

| 指标 | 类型 | Tags | 含义 |
|---|---|---|---|
| `liteflow.chain.executions` | Timer | `chain`, `status`(success/failed) | chain 执行次数、总/平均/最大耗时、成功数、失败数 |
| `liteflow.chain.active` | LongTaskTimer | `chain` | 当前在途执行数 + 最长在途耗时（发现卡住/堆积） |
| `liteflow.chain.errors` | Counter | `chain`, `exception`(异常类 simpleName) | 按异常类型分布的失败数 |

说明：
- `status` 由 chain 执行结束时 `slot.getException()` 是否为 `null` 判定。
- `exception` tag 取异常类的 `getClass().getSimpleName()`，基数有界。
- 同线程嵌套子链（before A → before B → after B → after A）通过每线程样本栈正确配对；WHEN 并行子链在各自线程，互不影响。

### 3.2 Node 级指标（按 `nodeId` 聚合）

| 指标 | 类型 | Tags | 含义 |
|---|---|---|---|
| `liteflow.node.executions` | Timer | `node`, `type`(NodeTypeEnum.name()), `status` | 组件执行次数、耗时分布、成功/失败 |
| `liteflow.node.active` | LongTaskTimer | `node` | 在途组件数（定位慢/挂起组件） |
| `liteflow.node.errors` | Counter | `node`, `exception` | 组件按异常类型的失败数 |

说明：
- `type` 取 `NodeComponent.getType().name()`，即 `COMMON` / `BOOLEAN` / `SWITCH` / `FOR` / `ITERATOR` / `SCRIPT` 等 `NodeTypeEnum` 枚举名。
- 耗时与异常直接来自节点执行的 `finally` 块，无需订阅方自行计时。

### 3.3 全局 / 注册表 Gauge（`LiteflowMeterBinder` 一次性绑定）

| 指标 | 来源 | 含义 |
|---|---|---|
| `liteflow.chains.registered` | `FlowBus.getChainMap().size()` | 已注册 chain 总数 |
| `liteflow.nodes.registered` | `FlowBus.getNodeMap().size()` | 已注册 node 总数 |
| `liteflow.slot.size` | `LiteflowConfig.slotSize` | slot 池容量 |
| `liteflow.slot.occupied` | `DataBus.OCCUPY_COUNT` | 当前在用 slot 数（关键饱和度，逼近容量即并发吃紧/泄漏） |

---

## 4. 结构端点 `/actuator/liteflow`

只读端点，全部为 `@ReadOperation`，数据由 `LiteflowMetaView` 提供（结构读 `FlowBus`，指标快照读 `MeterRegistry`）。它补齐了 Micrometer 看不到的定义信息（EL 原文、组件 class/type、未执行过的 chain/node 等）。

| 路由 | 返回 |
|---|---|
| `GET /actuator/liteflow` | 概览 |
| `GET /actuator/liteflow/chains` | 全部 chain 列表 |
| `GET /actuator/liteflow/nodes` | 全部 node 列表 |
| `GET /actuator/liteflow/chains/{chainId}` | 单 chain 详情 + 指标快照 |
| `GET /actuator/liteflow/nodes/{nodeId}` | 单 node 详情 + 指标快照 + 包含它的 chain 列表 |

> 指标快照是"尽力而为"：当 `liteflow.metrics.enabled=false` 或容器中没有 `MeterRegistry` 时，端点仍返回结构信息，只是 `metrics` 字段省略。

### 示例：概览

`GET /actuator/liteflow`

```json
{
  "chainsRegistered": 3,
  "nodesRegistered": 12,
  "slotOccupied": 1,
  "chainIds": ["mChain", "subChain", "ifChain"],
  "nodeIds": ["a", "b", "c", "ifNode"]
}
```

### 示例：chain 列表项

`GET /actuator/liteflow/chains`

```json
[
  {
    "chainId": "mChain",
    "namespace": "default",
    "el": "THEN(a, b);",
    "elMd5": "f1e2d3c4b5a6..."
  }
]
```

### 示例：单 chain 详情（含指标快照）

`GET /actuator/liteflow/chains/mChain`

```json
{
  "chainId": "mChain",
  "namespace": "default",
  "el": "THEN(a, b);",
  "elMd5": "f1e2d3c4b5a6...",
  "metrics": {
    "count": 1280,
    "failed": 3,
    "errorRate": 0.00234375,
    "meanMs": 5.6,
    "maxMs": 142.0
  }
}
```

### 示例：node 列表项

`GET /actuator/liteflow/nodes`

```json
[
  {
    "nodeId": "a",
    "name": "A组件",
    "type": "COMMON",
    "script": false,
    "clazz": "com.example.flow.AComponent",
    "language": null
  }
]
```

### 示例：单 node 详情（含指标快照与所在 chain）

`GET /actuator/liteflow/nodes/a`

```json
{
  "nodeId": "a",
  "name": "A组件",
  "type": "COMMON",
  "script": false,
  "clazz": "com.example.flow.AComponent",
  "language": null,
  "metrics": {
    "count": 1280,
    "failed": 0,
    "errorRate": 0.0,
    "meanMs": 2.1,
    "maxMs": 38.0
  },
  "inChains": ["mChain", "subChain"]
}
```

指标快照字段含义：`count` = 总执行次数；`failed` = 失败次数；`errorRate` = `failed / count`；`meanMs` = 平均耗时（毫秒）；`maxMs` = 最大耗时（毫秒）。

---

## 5. 分位 / 直方图

分位与直方图属于 Micrometer 标准配置，LiteFlow 不另立配置项。`Timer` 默认只发布 `count` / `sum` / `max`，无法直接倒推 P95 / P99，需要按下面任一方式开启：

### 客户端分位（进程内计算，默认发布到所有 registry）

适用于不想依赖 Prometheus histogram bucket、希望直接拿到分位值的场景：

```properties
# 为 chain 执行耗时启用客户端 P95 / P99
management.metrics.distribution.percentiles[liteflow.chain.executions]=0.95,0.99
# 为 node 执行耗时启用客户端 P95 / P99
management.metrics.distribution.percentiles[liteflow.node.executions]=0.95,0.99
```

### 直方图（发布 bucket，交 Prometheus 用 histogram_quantile 计算）

推荐用法，聚合更准确，且支持跨实例聚合：

```properties
# 为 chain 执行耗时发布 histogram bucket
management.metrics.distribution.percentiles-histogram[liteflow.chain.executions]=true
management.metrics.distribution.percentiles-histogram[liteflow.node.executions]=true
```

可选：自定义 SLO 边界（仅影响 bucket 切分）：

```properties
management.metrics.distribution.slo[liteflow.chain.executions]=50ms,100ms,500ms
```

### 过滤 / 裁剪指标

如果你想去掉某些指标（例如不想要 `active` 这类 LongTaskTimer），自定义一个 `MeterFilter` Bean 即可，这是 Micrometer 的标准机制：

```java
@Configuration
public class MyMeterFilterConfig {
    @Bean
    public MeterFilter dropActiveMetrics() {
        return MeterFilter.deny(id -> id.getName().endsWith(".active"));
    }
}
```

---

## 6. 常用 PromQL 与告警

Prometheus 抓取时，指标名会按 Micrometer 约定转换：

- Timer `liteflow.chain.executions` → `liteflow_chain_executions_seconds_count` / `_sum` / `_max` / `_bucket`
- Counter `liteflow.chain.errors` → `liteflow_chain_errors_total`
- LongTaskTimer `liteflow.chain.active` → `liteflow_chain_active_seconds_count` / `_sum` / `_max`
- Gauge `liteflow.slot.occupied` → `liteflow_slot_occupied`（tag 名保持原样：`chain` / `node` / `status` / `exception` / `type`）

下面示例假设 chain 名为 `mChain`、组件名为 `a`。

### QPS（每秒执行次数）

```promql
# chain QPS
rate(liteflow_chain_executions_seconds_count{chain="mChain"}[1m])

# node QPS
rate(liteflow_node_executions_seconds_count{node="a"}[1m])
```

### 平均耗时（秒）

```promql
rate(liteflow_chain_executions_seconds_sum{chain="mChain"}[1m])
  / rate(liteflow_chain_executions_seconds_count{chain="mChain"}[1m])
```

### 错误率

```promql
sum(rate(liteflow_chain_executions_seconds_count{chain="mChain", status="failed"}[5m]))
  /
sum(rate(liteflow_chain_executions_seconds_count{chain="mChain"}[5m]))
```

按异常类型分布的失败速率（`liteflow.chain.errors` Counter）：

```promql
sum by (exception) (rate(liteflow_chain_errors_total{chain="mChain"}[5m]))
```

### P95 耗时（需开启第 5 节的直方图）

```promql
histogram_quantile(0.95,
  sum by (le) (rate(liteflow_chain_executions_seconds_bucket{chain="mChain"}[5m])))
```

### slot 池饱和度

```promql
liteflow_slot_occupied / liteflow_slot_size
```

### 在途执行数（LongTaskTimer 的 active 数）

```promql
liteflow_chain_active_seconds_count{chain="mChain"}
```

### 常用告警规则示例

```yaml
groups:
  - name: liteflow
    rules:
      # chain 错误率 5 分钟内超过 5%
      - alert: LiteflowChainHighErrorRate
        expr: |
          sum by (chain) (rate(liteflow_chain_executions_seconds_count{status="failed"}[5m]))
            /
          sum by (chain) (rate(liteflow_chain_executions_seconds_count[5m]))
            > 0.05
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "LiteFlow chain {{ $labels.chain }} 错误率过高"

      # slot 池占用超过 80%
      - alert: LiteflowSlotSaturated
        expr: liteflow_slot_occupied / liteflow_slot_size > 0.8
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "LiteFlow slot 池饱和，可能并发吃紧或存在 slot 泄漏"
```

---

## 7. 职责归属与性能

### 7.1 谁算 QPS / 平均 / 分位

LiteFlow 核心**不做任何统计计算**，只上报原始测量值；聚合与推导全部交给 Micrometer 与监控后端：

| 层 | 职责 | 由谁承担 |
|---|---|---|
| LiteFlow 记录 | 每次执行结束 `timer.record(耗时)`；出错 `counter.increment()` | `ChainMetricsLifeCycle` / `NodeMetricsLifeCycle` 钩子，只记原始值 |
| Micrometer 聚合 | 内存中维护 `count`、`totalTime`(sum)、`max` | Micrometer `Timer`（`LongAdder`） |
| Prometheus / Grafana 推导 | QPS、平均耗时、错误率在**查询时**算出 | 监控后端 |

- **QPS**：不落库，由 `rate(count[1m])` 查询时对只增计数求斜率得到。
- **平均耗时**：不落库，由 `rate(sum[1m]) / rate(count[1m])` 当场相除。
- **P95 / P99**：count + sum 无法倒推，需在进程内开分位或发布 histogram bucket 交 Prometheus `histogram_quantile()`——这是 Micrometer / Prometheus 的标准机制，不是 LiteFlow 的能力。

这与现有的 `MonitorBus`（自存最近 N 条、自算平均、只打日志）有本质区别：本模块**零统计计算、零历史存储**，且与 `MonitorBus` 完全独立、互不影响。

### 7.2 性能影响与基数控制

**单次开销：**
- **未引入 `liteflow-metrics` 时接近零。** chain 钩子调用本就存在于 `Chain.execute()`；node 新钩子在 `NodeComponent.execute()` 的 `finally` 中只多一次"空列表"判断，不分配、不遍历。
- **引入后亚微秒级，通常 < 1%。** 每次执行新增一次 `timer.record(...)`（一次按 name+tags 的 `ConcurrentHashMap` 查找 + 几次 `LongAdder.add`，量级约几十到一两百纳秒）、一个小样本对象、每线程栈的一次入/出栈；出错时再有一次 Counter 自增。相对 `NodeComponent.execute()` 本就存在的 `CmpStep`、`StopWatch`、两次 `LOG.info`，以及用户业务逻辑（常含 DB / IO），新增开销可忽略。

**真正的风险点是时间序列基数：**
- 每个 `(chain,status)` / `(node,type,status)` 组合驻留一个小 Meter（几百字节）并导出一条时间序列。
- 选定的 **chain + node 两级** → 内存 ≈ O(chain 数 + node 数)，几千个量级仅几 MB。
- 已规避高基数陷阱：未采用"node-in-chain 三级"组合；`exception` tag 只取类 simpleName，基数有界；不含 requestId、完整异常 message 等。
- 超高吞吐场景下若需进一步压成本，`active`（LongTaskTimer）是最可做成可选 / 移除的一项。

---

## 附录：快速核对清单

- [ ] 已引入 `liteflow-spring-boot-starter`（或 Boot4 版）+ `spring-boot-starter-actuator` + 一个 registry（如 `micrometer-registry-prometheus`）；
- [ ] `management.endpoints.web.exposure.include` 包含 `liteflow,prometheus`；
- [ ] 访问 `GET /actuator/liteflow` 能看到 chain / node 注册数；
- [ ] 执行一次 chain 后，`GET /actuator/liteflow/chains/{chainId}` 的 `metrics.count` 有变化；
- [ ] （可选）开启 `management.metrics.distribution.percentiles-histogram[liteflow.chain.executions]=true` 以支持 P95 / P99；
- [ ] 在 Grafana 用 `rate(liteflow_chain_executions_seconds_count{chain="..."}[1m])` 看到 QPS 曲线。
