# LiteFlow Actuator 指标端点 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 给 LiteFlow 增加一套 Micrometer 指标（chain/node 两级 + 注册表/slot Gauge）和一个只读结构端点 `/actuator/liteflow`，对接 Prometheus/Grafana。

**Architecture:** core 新增一个 list 形扩展点 `PostProcessNodeExecuteLifeCycle`（与现有 `PostProcessChainExecuteLifeCycle` 对称），新建框架无关模块 `liteflow-metrics` 实现两个 lifecycle + 注册表 Gauge + 端点取数 POJO，最后在 Boot2/3 与 Boot4 两个 starter 里各加薄壳 AutoConfiguration + `@Endpoint`。LiteFlow 只上报原始测量值，统计聚合（QPS/平均/分位）交给 Micrometer 与 Prometheus。

**Tech Stack:** Java 8、Maven（flatten `${revision}`）、Micrometer 1.8.x（`micrometer-core`）、Spring Boot 2.6.8 actuator、JUnit 5 (Jupiter)。

## Global Constraints

- 版本占位：所有新模块 pom 用 `${revision}`（当前 `2.16.0`），parent 为 `com.yomahub:liteflow`。
- `liteflow-metrics` 必须 Java 8 可编译（进 `compile-8-to-16` 主集），**不得依赖 Spring/Solon**，只依赖 `liteflow-core` + `micrometer-core`(optional)。
- core 改动为纯增量：扩展点列表为空时零开销，不得改变既有行为，不得触碰 `MonitorBus` 路径。
- 指标命名前缀 `liteflow.`；tag 保持低基数：`chain`、`node`、`type`、`status`(success/failed)、`exception`(异常类 `getSimpleName()`)。**禁止**把 requestId、完整异常 message 等高基数值放进 tag。
- 采集粒度只到 chain + node 两级，**不做** (chain,node) 组合维度。
- 运行测试：根 pom surefire 默认 `skipTests`，跑测试须显式 `-DskipTests=false`。
- JUnit 用 `org.junit.jupiter.api.*`（`@Test`/`Assertions`）。

---

## File Structure

**core（修改/新增）**
- Create `liteflow-core/src/main/java/com/yomahub/liteflow/lifecycle/PostProcessNodeExecuteLifeCycle.java` — 新扩展点接口。
- Modify `liteflow-core/src/main/java/com/yomahub/liteflow/lifecycle/LifeCycleHolder.java` — 加 list、分支、getter、clean。
- Modify `liteflow-core/src/main/java/com/yomahub/liteflow/core/NodeComponent.java:95-166` — finally 中触发新钩子。

**liteflow-metrics（新模块，框架无关）**
- Create `liteflow-metrics/pom.xml`
- Create `liteflow-metrics/src/main/java/com/yomahub/liteflow/metrics/LiteflowMeterBinder.java` — 注册表/slot Gauge。
- Create `liteflow-metrics/src/main/java/com/yomahub/liteflow/metrics/ChainMetricsLifeCycle.java` — chain 指标。
- Create `liteflow-metrics/src/main/java/com/yomahub/liteflow/metrics/NodeMetricsLifeCycle.java` — node 指标。
- Create `liteflow-metrics/src/main/java/com/yomahub/liteflow/metrics/LiteflowMetaView.java` — 结构 + 指标快照取数。
- Tests under `liteflow-metrics/src/test/java/com/yomahub/liteflow/metrics/`。

**root pom（修改）**
- Modify `pom.xml` — 三个 profile（`compile-8-to-16`、`compile-17+`、`release-on-8`）加 `<module>liteflow-metrics</module>`；dependencyManagement 加 `micrometer-core`。

**Boot2/3 starter**
- Create `liteflow-spring-boot-starter/src/main/java/com/yomahub/liteflow/springboot/metrics/LiteflowMetricsAutoConfiguration.java`
- Create `liteflow-spring-boot-starter/src/main/java/com/yomahub/liteflow/springboot/metrics/LiteflowEndpoint.java`
- Modify `liteflow-spring-boot-starter/pom.xml`、`META-INF/spring.factories`、`META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`
- Test in `liteflow-testcase-el/liteflow-testcase-el-springboot`。

**Boot4 starter**
- Create `liteflow-spring-boot4-starter/src/main/java/com/yomahub/liteflow/springboot4/metrics/LiteflowMetricsAutoConfiguration.java`
- Create `liteflow-spring-boot4-starter/src/main/java/com/yomahub/liteflow/springboot4/metrics/LiteflowEndpoint.java`
- Modify `liteflow-spring-boot4-starter/pom.xml`、`META-INF/spring/...AutoConfiguration.imports`
- Test in `liteflow-testcase-el/liteflow-testcase-el-springboot4`。

**docs**
- Create `docs/liteflow-metrics-guide.md`

---

## Task 1: core 新增 `PostProcessNodeExecuteLifeCycle` 扩展点

**Files:**
- Create: `liteflow-core/src/main/java/com/yomahub/liteflow/lifecycle/PostProcessNodeExecuteLifeCycle.java`
- Modify: `liteflow-core/src/main/java/com/yomahub/liteflow/lifecycle/LifeCycleHolder.java`
- Modify: `liteflow-core/src/main/java/com/yomahub/liteflow/core/NodeComponent.java:95-166`
- Test (新建测试切片): `liteflow-testcase-el/liteflow-testcase-el-springboot/src/test/java/com/yomahub/liteflow/test/nodeexecute/` 下若干 + `resources/nodeexecute/`

**Interfaces:**
- Produces:
  - `interface PostProcessNodeExecuteLifeCycle extends LifeCycle`，方法 `void postProcessBeforeNodeExecute(NodeComponent cmp)` 与 `void postProcessAfterNodeExecute(NodeComponent cmp, long timeSpent, Exception e)`（`e` 成功时为 null）。
  - `LifeCycleHolder.getPostProcessNodeExecuteLifeCycleList()` → `List<PostProcessNodeExecuteLifeCycle>`。

- [ ] **Step 1: 写失败测试 —— 收集器 + lifecycle 实现 + 组件 + 规则 + 测试类**

Create `liteflow-testcase-el/liteflow-testcase-el-springboot/src/test/java/com/yomahub/liteflow/test/nodeexecute/NodeExecuteCollector.java`:

```java
package com.yomahub.liteflow.test.nodeexecute;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** 测试用静态收集器，记录节点执行钩子的回调 */
public class NodeExecuteCollector {

    public static class Record {
        public final String nodeId;
        public final long timeSpent;
        public final Exception exception;
        public Record(String nodeId, long timeSpent, Exception exception) {
            this.nodeId = nodeId;
            this.timeSpent = timeSpent;
            this.exception = exception;
        }
    }

    public static final List<String> BEFORE = new CopyOnWriteArrayList<>();
    public static final List<Record> AFTER = new CopyOnWriteArrayList<>();

    public static void clear() {
        BEFORE.clear();
        AFTER.clear();
    }

    public static List<Record> afters() {
        return Collections.unmodifiableList(AFTER);
    }
}
```

Create `.../nodeexecute/TestNodeExecuteLifeCycle.java`:

```java
package com.yomahub.liteflow.test.nodeexecute;

import com.yomahub.liteflow.core.NodeComponent;
import com.yomahub.liteflow.lifecycle.PostProcessNodeExecuteLifeCycle;
import org.springframework.stereotype.Component;

@Component
public class TestNodeExecuteLifeCycle implements PostProcessNodeExecuteLifeCycle {
    @Override
    public void postProcessBeforeNodeExecute(NodeComponent cmp) {
        NodeExecuteCollector.BEFORE.add(cmp.getNodeId());
    }
    @Override
    public void postProcessAfterNodeExecute(NodeComponent cmp, long timeSpent, Exception e) {
        NodeExecuteCollector.AFTER.add(new NodeExecuteCollector.Record(cmp.getNodeId(), timeSpent, e));
    }
}
```

Create `.../nodeexecute/cmp/NeACmp.java`:

```java
package com.yomahub.liteflow.test.nodeexecute.cmp;

import com.yomahub.liteflow.core.NodeComponent;
import org.springframework.stereotype.Component;

@Component("neA")
public class NeACmp extends NodeComponent {
    @Override
    public void process() {
        // 正常组件
    }
}
```

Create `.../nodeexecute/cmp/NeBoomCmp.java`:

```java
package com.yomahub.liteflow.test.nodeexecute.cmp;

import com.yomahub.liteflow.core.NodeComponent;
import org.springframework.stereotype.Component;

@Component("neBoom")
public class NeBoomCmp extends NodeComponent {
    @Override
    public void process() {
        throw new IllegalStateException("boom");
    }
}
```

Create `liteflow-testcase-el/liteflow-testcase-el-springboot/src/test/resources/nodeexecute/flow.el.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE flow PUBLIC  "liteflow" "liteflow.dtd">
<flow>
    <chain name="okChain">
        THEN(neA, neA);
    </chain>
    <chain name="boomChain">
        THEN(neA, neBoom);
    </chain>
</flow>
```

Create `liteflow-testcase-el/liteflow-testcase-el-springboot/src/test/resources/nodeexecute/application.properties`:

```properties
liteflow.rule-source=nodeexecute/flow.el.xml
```

Create `.../nodeexecute/NodeExecuteLifeCycleSpringbootTest.java`:

```java
package com.yomahub.liteflow.test.nodeexecute;

import com.yomahub.liteflow.core.FlowExecutor;
import com.yomahub.liteflow.flow.LiteflowResponse;
import com.yomahub.liteflow.test.BaseTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.TestPropertySource;

import javax.annotation.Resource;

@TestPropertySource(value = "classpath:/nodeexecute/application.properties")
@SpringBootTest(classes = NodeExecuteLifeCycleSpringbootTest.class)
@EnableAutoConfiguration
@ComponentScan({ "com.yomahub.liteflow.test.nodeexecute" })
public class NodeExecuteLifeCycleSpringbootTest extends BaseTest {

    @Resource
    private FlowExecutor flowExecutor;

    @BeforeEach
    public void setUp() {
        NodeExecuteCollector.clear();
    }

    @Test
    public void testSuccessHooks() {
        LiteflowResponse response = flowExecutor.execute2Resp("okChain", "arg");
        Assertions.assertTrue(response.isSuccess());
        // okChain = THEN(neA, neA) → 2 次节点执行
        Assertions.assertEquals(2, NodeExecuteCollector.BEFORE.size());
        Assertions.assertEquals(2, NodeExecuteCollector.afters().size());
        for (NodeExecuteCollector.Record r : NodeExecuteCollector.afters()) {
            Assertions.assertEquals("neA", r.nodeId);
            Assertions.assertNull(r.exception);
            Assertions.assertTrue(r.timeSpent >= 0);
        }
    }

    @Test
    public void testErrorHookCarriesException() {
        LiteflowResponse response = flowExecutor.execute2Resp("boomChain", "arg");
        Assertions.assertFalse(response.isSuccess());
        // boomChain = THEN(neA, neBoom)：neA 成功，neBoom 抛异常
        Assertions.assertEquals(2, NodeExecuteCollector.afters().size());
        NodeExecuteCollector.Record boom = NodeExecuteCollector.afters().get(1);
        Assertions.assertEquals("neBoom", boom.nodeId);
        Assertions.assertNotNull(boom.exception);
        Assertions.assertEquals(IllegalStateException.class, boom.exception.getClass());
    }
}
```

- [ ] **Step 2: 跑测试确认失败（编译失败）**

Run: `mvn test -pl liteflow-testcase-el/liteflow-testcase-el-springboot -Dtest=NodeExecuteLifeCycleSpringbootTest -DskipTests=false`
Expected: 编译失败，`cannot find symbol: class PostProcessNodeExecuteLifeCycle`。

- [ ] **Step 3: 创建扩展点接口**

Create `liteflow-core/src/main/java/com/yomahub/liteflow/lifecycle/PostProcessNodeExecuteLifeCycle.java`:

```java
package com.yomahub.liteflow.lifecycle;

import com.yomahub.liteflow.core.NodeComponent;

/**
 * 生命周期接口
 * 执行单个组件（Node）的时候
 *
 * @author Bryan.Zhang
 */
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

- [ ] **Step 4: `LifeCycleHolder` 增补 list / 分支 / getter / clean**

Modify `liteflow-core/src/main/java/com/yomahub/liteflow/lifecycle/LifeCycleHolder.java`.

在 `POST_PROCESS_CHAIN_EXECUTE_LIFE_CYCLE_LIST` 声明之后新增：

```java
    private static final List<PostProcessNodeExecuteLifeCycle> POST_PROCESS_NODE_EXECUTE_LIFE_CYCLE_LIST = new ArrayList<>();
```

在 `addLifeCycle` 的 if-else 链最后一个分支后追加：

```java
        }else if(PostProcessNodeExecuteLifeCycle.class.isAssignableFrom(lifeCycle.getClass())){
            POST_PROCESS_NODE_EXECUTE_LIFE_CYCLE_LIST.add((PostProcessNodeExecuteLifeCycle)lifeCycle);
```

（即把原 `PostProcessChainExecuteLifeCycle` 分支的闭合 `}` 改为 `}else if(...){ ... }`。）

在 `getPostProcessChainExecuteLifeCycleList()` 方法后新增：

```java
    public static List<PostProcessNodeExecuteLifeCycle> getPostProcessNodeExecuteLifeCycleList() {
        return POST_PROCESS_NODE_EXECUTE_LIFE_CYCLE_LIST;
    }
```

在 `clean()` 方法体内追加一行：

```java
        POST_PROCESS_NODE_EXECUTE_LIFE_CYCLE_LIST.clear();
```

- [ ] **Step 5: `NodeComponent.execute()` 接入新钩子**

Modify `liteflow-core/src/main/java/com/yomahub/liteflow/core/NodeComponent.java`。

5a. 在 import 区（第 22 行 `import ...flow.element.Condition;` 附近）加入：

```java
import com.yomahub.liteflow.lifecycle.LifeCycleHolder;
import com.yomahub.liteflow.lifecycle.PostProcessNodeExecuteLifeCycle;
```

5b. 把 `execute()` 方法（当前 95-166 行）整体替换为下面版本（新增：try 前声明 `nodeExecuteException`、try 起始处 before 钩子、catch 中赋值、finally 末尾 after 钩子）：

```java
	public void execute() throws Exception {
		Slot slot = this.getSlot();

		// 在元数据里加入step信息
		CmpStep cmpStep = new CmpStep(nodeId, name, CmpStepTypeEnum.SINGLE);
		cmpStep.setTag(this.getTag());
		cmpStep.setInstance(this);
		cmpStep.setRefNode(this.getRefNode());
		cmpStep.setStartTime(new Date());
		cmpStep.setThreadName(Thread.currentThread().getName());
		cmpStep.setChainId(this.getRefNode().getCurrChainId());
		cmpStep.setLoopIndex(this.getRefNode().getLoopIndex());
		slot.addStep(cmpStep);

		StopWatch stopWatch = new StopWatch();
		stopWatch.start();

		// 节点执行异常引用，供 finally 中的生命周期钩子使用（成功为 null）
		Exception nodeExecuteException = null;

		// 节点执行生命周期（前）——list 形，可叠加，与 monitorBus 独立
		List<PostProcessNodeExecuteLifeCycle> nodeExecuteLifeCycleList = LifeCycleHolder.getPostProcessNodeExecuteLifeCycleList();
		if (!nodeExecuteLifeCycleList.isEmpty()) {
			nodeExecuteLifeCycleList.forEach(lc -> lc.postProcessBeforeNodeExecute(self));
		}

		try {
			LOG.info("[O]start component[{}] execution", self.getDisplayName());

			// 前置处理
			self.beforeProcess();

			// 主要的处理逻辑
			self.process();

			// 成功后回调方法
			self.onSuccess();

			// 步骤状态设为true
			cmpStep.setSuccess(true);
		}
		catch (Exception e) {
			// 步骤状态设为false，并加入异常
			cmpStep.setSuccess(false);
			cmpStep.setException(e);
			nodeExecuteException = e;

			// 执行失败后回调方法
			// 这里要注意，失败方法本身抛出错误，只打出堆栈，往外抛出的还是主要的异常
			try {
				self.onError(e);
			}
			catch (Exception ex) {
				String errMsg = StrUtil.format("component[{}] onError method happens exception", this.getDisplayName());
				LOG.error(errMsg, ex);
			}
			throw e;
		}
		finally {
			// 后置处理
			self.afterProcess();

			stopWatch.stop();
			final long timeSpent = stopWatch.getTotalTimeMillis();
			LOG.info("component[{}] finished in {} milliseconds", this.getDisplayName(), timeSpent);

			// 步骤自定义数据设置
			cmpStep.setStepData(this.getRefNode().getStepData());

			// 结束时间设置
			cmpStep.setEndTime(new Date());

			// 往CmpStep中放入时间消耗信息
			cmpStep.setTimeSpent(timeSpent);

			// 性能统计
			if (ObjectUtil.isNotNull(monitorBus)) {
				CompStatistics statistics = new CompStatistics(this.getClass().getSimpleName(), timeSpent);
				monitorBus.addStatistics(statistics);
			}

			// 节点执行生命周期（后）——带上耗时与异常
			if (!nodeExecuteLifeCycleList.isEmpty()) {
				final Exception finalEx = nodeExecuteException;
				nodeExecuteLifeCycleList.forEach(lc -> lc.postProcessAfterNodeExecute(self, timeSpent, finalEx));
			}
		}
	}
```

- [ ] **Step 6: 跑测试确认通过**

Run: `mvn test -pl liteflow-testcase-el/liteflow-testcase-el-springboot -Dtest=NodeExecuteLifeCycleSpringbootTest -DskipTests=false`
Expected: PASS（2 个测试方法）。若首次因 core 改动未编进本地仓库失败，先 `mvn install -pl liteflow-core -DskipTests` 再重跑。

- [ ] **Step 7: 回归 —— 确认既有 lifecycle 测试不受影响**

Run: `mvn test -pl liteflow-testcase-el/liteflow-testcase-el-springboot -Dtest=LifeCycleSpringbootTest -DskipTests=false`
Expected: PASS。

- [ ] **Step 8: Commit**

```bash
git add liteflow-core/src/main/java/com/yomahub/liteflow/lifecycle/PostProcessNodeExecuteLifeCycle.java \
        liteflow-core/src/main/java/com/yomahub/liteflow/lifecycle/LifeCycleHolder.java \
        liteflow-core/src/main/java/com/yomahub/liteflow/core/NodeComponent.java \
        liteflow-testcase-el/liteflow-testcase-el-springboot/src/test/java/com/yomahub/liteflow/test/nodeexecute \
        liteflow-testcase-el/liteflow-testcase-el-springboot/src/test/resources/nodeexecute
git commit -m "feat(core): 新增 PostProcessNodeExecuteLifeCycle 节点执行生命周期扩展点"
```

---

## Task 2: 新建 `liteflow-metrics` 模块骨架 + 注册表/slot Gauge

**Files:**
- Modify: `pom.xml`（三个 profile 加 module + dependencyManagement 加 micrometer）
- Create: `liteflow-metrics/pom.xml`
- Create: `liteflow-metrics/src/main/java/com/yomahub/liteflow/metrics/LiteflowMeterBinder.java`
- Test: `liteflow-metrics/src/test/java/com/yomahub/liteflow/metrics/LiteflowMeterBinderTest.java`

**Interfaces:**
- Consumes: `FlowBus.getChainMap()` / `getNodeMap()`（`Map`，有 `.size()`）、`DataBus.OCCUPY_COUNT`（`AtomicInteger`）、`LiteflowConfig.getSlotSize()`（`Integer`）。
- Produces: `class LiteflowMeterBinder implements io.micrometer.core.instrument.binder.MeterBinder`，构造 `LiteflowMeterBinder(LiteflowConfig config)`，`bindTo(MeterRegistry)` 注册 4 个 Gauge：`liteflow.chains.registered`、`liteflow.nodes.registered`、`liteflow.slot.size`、`liteflow.slot.occupied`。

- [ ] **Step 1: root pom 加 micrometer 版本与依赖管理**

Modify `pom.xml`：在 `<properties>` 区（`<revision>2.16.0</revision>` 附近）加：

```xml
		<micrometer.version>1.8.5</micrometer.version>
```

在 `<dependencyManagement><dependencies>` 区加：

```xml
			<dependency>
				<groupId>io.micrometer</groupId>
				<artifactId>micrometer-core</artifactId>
				<version>${micrometer.version}</version>
			</dependency>
```

- [ ] **Step 2: root pom 三个 profile 注册新模块**

Modify `pom.xml`：分别在 `compile-8-to-16`、`compile-17+`、`release-on-8` 三个 profile 的 `<modules>` 中，`<module>liteflow-el-builder</module>` 之后追加一行：

```xml
					<module>liteflow-metrics</module>
```

- [ ] **Step 3: 创建模块 pom**

Create `liteflow-metrics/pom.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <packaging>jar</packaging>
    <artifactId>liteflow-metrics</artifactId>
    <name>${project.artifactId}</name>
    <description>liteflow micrometer metrics</description>

    <parent>
        <artifactId>liteflow</artifactId>
        <groupId>com.yomahub</groupId>
        <version>${revision}</version>
        <relativePath>../pom.xml</relativePath>
    </parent>

    <dependencies>
        <dependency>
            <groupId>com.yomahub</groupId>
            <artifactId>liteflow-core</artifactId>
            <version>${revision}</version>
        </dependency>
        <dependency>
            <groupId>io.micrometer</groupId>
            <artifactId>micrometer-core</artifactId>
            <optional>true</optional>
        </dependency>

        <!-- test -->
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

</project>
```

- [ ] **Step 4: 写失败测试**

Create `liteflow-metrics/src/test/java/com/yomahub/liteflow/metrics/LiteflowMeterBinderTest.java`:

```java
package com.yomahub.liteflow.metrics;

import com.yomahub.liteflow.flow.FlowBus;
import com.yomahub.liteflow.property.LiteflowConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class LiteflowMeterBinderTest {

    @Test
    public void testGauges() {
        LiteflowConfig config = new LiteflowConfig();
        config.setSlotSize(2048);

        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        new LiteflowMeterBinder(config).bindTo(registry);

        // slot.size 反映配置
        Assertions.assertEquals(2048.0,
                registry.get("liteflow.slot.size").gauge().value());

        // chains.registered 反映 FlowBus 当前大小
        double before = registry.get("liteflow.chains.registered").gauge().value();
        FlowBus.addChain("metricsTestChain");
        double after = registry.get("liteflow.chains.registered").gauge().value();
        Assertions.assertEquals(before + 1, after);

        // 其余两个 Gauge 存在
        Assertions.assertNotNull(registry.get("liteflow.nodes.registered").gauge());
        Assertions.assertNotNull(registry.get("liteflow.slot.occupied").gauge());
    }
}
```

- [ ] **Step 5: 跑测试确认失败**

Run: `mvn test -pl liteflow-metrics -Dtest=LiteflowMeterBinderTest -DskipTests=false`
Expected: 编译失败，`cannot find symbol: class LiteflowMeterBinder`。

- [ ] **Step 6: 实现 `LiteflowMeterBinder`**

Create `liteflow-metrics/src/main/java/com/yomahub/liteflow/metrics/LiteflowMeterBinder.java`:

```java
package com.yomahub.liteflow.metrics;

import com.yomahub.liteflow.flow.FlowBus;
import com.yomahub.liteflow.property.LiteflowConfig;
import com.yomahub.liteflow.slot.DataBus;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;

/**
 * LiteFlow 注册表 / slot 池的 Gauge 绑定
 *
 * @author Bryan.Zhang
 */
public class LiteflowMeterBinder implements MeterBinder {

    private final LiteflowConfig liteflowConfig;

    public LiteflowMeterBinder(LiteflowConfig liteflowConfig) {
        this.liteflowConfig = liteflowConfig;
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        Gauge.builder("liteflow.chains.registered", FlowBus.getChainMap(), m -> m.size())
                .description("已注册的 chain 数量")
                .register(registry);

        Gauge.builder("liteflow.nodes.registered", FlowBus.getNodeMap(), m -> m.size())
                .description("已注册的 node 数量")
                .register(registry);

        Gauge.builder("liteflow.slot.size", liteflowConfig, c -> {
                    Integer s = c.getSlotSize();
                    return s == null ? 0 : s;
                })
                .description("slot 池容量")
                .register(registry);

        Gauge.builder("liteflow.slot.occupied", DataBus.OCCUPY_COUNT, java.util.concurrent.atomic.AtomicInteger::get)
                .description("当前在用 slot 数")
                .register(registry);
    }
}
```

- [ ] **Step 7: 跑测试确认通过**

Run: `mvn test -pl liteflow-metrics -Dtest=LiteflowMeterBinderTest -DskipTests=false`
Expected: PASS。（若 `liteflow-core` 未装本地仓库：先 `mvn install -pl liteflow-core -DskipTests`。）

- [ ] **Step 8: Commit**

```bash
git add pom.xml liteflow-metrics/pom.xml \
        liteflow-metrics/src/main/java/com/yomahub/liteflow/metrics/LiteflowMeterBinder.java \
        liteflow-metrics/src/test/java/com/yomahub/liteflow/metrics/LiteflowMeterBinderTest.java
git commit -m "feat(metrics): 新建 liteflow-metrics 模块与注册表/slot Gauge"
```

---

## Task 3: `ChainMetricsLifeCycle` —— chain 级指标

**Files:**
- Create: `liteflow-metrics/src/main/java/com/yomahub/liteflow/metrics/ChainMetricsLifeCycle.java`
- Test: `liteflow-metrics/src/test/java/com/yomahub/liteflow/metrics/ChainMetricsLifeCycleTest.java`

**Interfaces:**
- Consumes: `PostProcessChainExecuteLifeCycle`（`postProcessBeforeChainExecute(String, Slot)` / `postProcessAfterChainExecute(String, Slot)`）、`Slot.getException()`。
- Produces: `class ChainMetricsLifeCycle implements PostProcessChainExecuteLifeCycle`，构造 `ChainMetricsLifeCycle(MeterRegistry)`。注册 `liteflow.chain.executions`(Timer, tags `chain`,`status`)、`liteflow.chain.active`(LongTaskTimer, tag `chain`)、`liteflow.chain.errors`(Counter, tags `chain`,`exception`)。

- [ ] **Step 1: 写失败测试**

Create `liteflow-metrics/src/test/java/com/yomahub/liteflow/metrics/ChainMetricsLifeCycleTest.java`:

```java
package com.yomahub.liteflow.metrics;

import com.yomahub.liteflow.slot.Slot;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ChainMetricsLifeCycleTest {

    @Test
    public void testSuccessTimer() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ChainMetricsLifeCycle lc = new ChainMetricsLifeCycle(registry);

        Slot slot = new Slot();
        lc.postProcessBeforeChainExecute("cA", slot);
        lc.postProcessAfterChainExecute("cA", slot);

        Assertions.assertEquals(1,
                registry.get("liteflow.chain.executions")
                        .tags("chain", "cA", "status", "success").timer().count());
    }

    @Test
    public void testFailureTimerAndErrorCounter() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ChainMetricsLifeCycle lc = new ChainMetricsLifeCycle(registry);

        Slot slot = new Slot();
        slot.setException(new IllegalArgumentException("bad"));
        lc.postProcessBeforeChainExecute("cB", slot);
        lc.postProcessAfterChainExecute("cB", slot);

        Assertions.assertEquals(1,
                registry.get("liteflow.chain.executions")
                        .tags("chain", "cB", "status", "failed").timer().count());
        Assertions.assertEquals(1.0,
                registry.get("liteflow.chain.errors")
                        .tags("chain", "cB", "exception", "IllegalArgumentException").counter().count());
    }

    @Test
    public void testNestedChainsLifo() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ChainMetricsLifeCycle lc = new ChainMetricsLifeCycle(registry);

        Slot slot = new Slot();
        // before outer → before inner → after inner → after outer
        lc.postProcessBeforeChainExecute("outer", slot);
        lc.postProcessBeforeChainExecute("inner", slot);
        lc.postProcessAfterChainExecute("inner", slot);
        lc.postProcessAfterChainExecute("outer", slot);

        Assertions.assertEquals(1,
                registry.get("liteflow.chain.executions").tags("chain", "inner", "status", "success").timer().count());
        Assertions.assertEquals(1,
                registry.get("liteflow.chain.executions").tags("chain", "outer", "status", "success").timer().count());
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `mvn test -pl liteflow-metrics -Dtest=ChainMetricsLifeCycleTest -DskipTests=false`
Expected: 编译失败，`cannot find symbol: class ChainMetricsLifeCycle`。

- [ ] **Step 3: 实现 `ChainMetricsLifeCycle`**

Create `liteflow-metrics/src/main/java/com/yomahub/liteflow/metrics/ChainMetricsLifeCycle.java`:

```java
package com.yomahub.liteflow.metrics;

import com.yomahub.liteflow.lifecycle.PostProcessChainExecuteLifeCycle;
import com.yomahub.liteflow.slot.Slot;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * chain 级指标采集（执行次数/耗时/在途/错误）
 *
 * @author Bryan.Zhang
 */
public class ChainMetricsLifeCycle implements PostProcessChainExecuteLifeCycle {

    private final MeterRegistry registry;

    /** 每线程的样本栈，支持同线程嵌套子链（LIFO） */
    private static final ThreadLocal<Deque<ChainSample>> SAMPLES =
            ThreadLocal.withInitial(ArrayDeque::new);

    public ChainMetricsLifeCycle(MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void postProcessBeforeChainExecute(String chainId, Slot slot) {
        Timer.Sample timerSample = Timer.start(registry);
        LongTaskTimer.Sample activeSample = LongTaskTimer.builder("liteflow.chain.active")
                .tag("chain", chainId)
                .register(registry)
                .start();
        SAMPLES.get().push(new ChainSample(timerSample, activeSample));
    }

    @Override
    public void postProcessAfterChainExecute(String chainId, Slot slot) {
        Deque<ChainSample> stack = SAMPLES.get();
        ChainSample sample = stack.poll();
        try {
            if (sample == null) {
                return;
            }
            Exception ex = slot.getException();
            String status = (ex == null) ? "success" : "failed";

            sample.timerSample.stop(Timer.builder("liteflow.chain.executions")
                    .tag("chain", chainId)
                    .tag("status", status)
                    .register(registry));
            sample.activeSample.stop();

            if (ex != null) {
                registry.counter("liteflow.chain.errors",
                        "chain", chainId,
                        "exception", ex.getClass().getSimpleName()).increment();
            }
        } finally {
            if (stack.isEmpty()) {
                SAMPLES.remove();
            }
        }
    }

    private static final class ChainSample {
        final Timer.Sample timerSample;
        final LongTaskTimer.Sample activeSample;
        ChainSample(Timer.Sample timerSample, LongTaskTimer.Sample activeSample) {
            this.timerSample = timerSample;
            this.activeSample = activeSample;
        }
    }
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `mvn test -pl liteflow-metrics -Dtest=ChainMetricsLifeCycleTest -DskipTests=false`
Expected: PASS（3 个方法）。

- [ ] **Step 5: Commit**

```bash
git add liteflow-metrics/src/main/java/com/yomahub/liteflow/metrics/ChainMetricsLifeCycle.java \
        liteflow-metrics/src/test/java/com/yomahub/liteflow/metrics/ChainMetricsLifeCycleTest.java
git commit -m "feat(metrics): 新增 ChainMetricsLifeCycle chain 级指标采集"
```

---

## Task 4: `NodeMetricsLifeCycle` —— node 级指标

**Files:**
- Create: `liteflow-metrics/src/main/java/com/yomahub/liteflow/metrics/NodeMetricsLifeCycle.java`
- Test: `liteflow-metrics/src/test/java/com/yomahub/liteflow/metrics/NodeMetricsLifeCycleTest.java`

**Interfaces:**
- Consumes: `PostProcessNodeExecuteLifeCycle`（Task 1 产出）、`NodeComponent.getNodeId()` / `getType()`（`NodeTypeEnum`，`.name()`）。
- Produces: `class NodeMetricsLifeCycle implements PostProcessNodeExecuteLifeCycle`，构造 `NodeMetricsLifeCycle(MeterRegistry)`。注册 `liteflow.node.executions`(Timer, tags `node`,`type`,`status`)、`liteflow.node.active`(LongTaskTimer, tag `node`)、`liteflow.node.errors`(Counter, tags `node`,`exception`)。

- [ ] **Step 1: 写失败测试**

Create `liteflow-metrics/src/test/java/com/yomahub/liteflow/metrics/NodeMetricsLifeCycleTest.java`:

```java
package com.yomahub.liteflow.metrics;

import com.yomahub.liteflow.core.NodeComponent;
import com.yomahub.liteflow.enums.NodeTypeEnum;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

public class NodeMetricsLifeCycleTest {

    static class FakeCmp extends NodeComponent {
        @Override
        public void process() { }
    }

    private NodeComponent cmp(String nodeId) {
        NodeComponent c = new FakeCmp();
        c.setNodeId(nodeId);
        c.setType(NodeTypeEnum.COMMON);
        return c;
    }

    @Test
    public void testSuccessTimer() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        NodeMetricsLifeCycle lc = new NodeMetricsLifeCycle(registry);

        NodeComponent c = cmp("nA");
        lc.postProcessBeforeNodeExecute(c);
        lc.postProcessAfterNodeExecute(c, 12L, null);

        Assertions.assertEquals(1,
                registry.get("liteflow.node.executions")
                        .tags("node", "nA", "type", "COMMON", "status", "success").timer().count());
        Assertions.assertEquals(12.0,
                registry.get("liteflow.node.executions")
                        .tags("node", "nA", "type", "COMMON", "status", "success")
                        .timer().totalTime(TimeUnit.MILLISECONDS), 0.001);
    }

    @Test
    public void testFailureTimerAndErrorCounter() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        NodeMetricsLifeCycle lc = new NodeMetricsLifeCycle(registry);

        NodeComponent c = cmp("nB");
        lc.postProcessBeforeNodeExecute(c);
        lc.postProcessAfterNodeExecute(c, 5L, new IllegalStateException("boom"));

        Assertions.assertEquals(1,
                registry.get("liteflow.node.executions")
                        .tags("node", "nB", "type", "COMMON", "status", "failed").timer().count());
        Assertions.assertEquals(1.0,
                registry.get("liteflow.node.errors")
                        .tags("node", "nB", "exception", "IllegalStateException").counter().count());
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `mvn test -pl liteflow-metrics -Dtest=NodeMetricsLifeCycleTest -DskipTests=false`
Expected: 编译失败，`cannot find symbol: class NodeMetricsLifeCycle`。

- [ ] **Step 3: 实现 `NodeMetricsLifeCycle`**

Create `liteflow-metrics/src/main/java/com/yomahub/liteflow/metrics/NodeMetricsLifeCycle.java`:

```java
package com.yomahub.liteflow.metrics;

import com.yomahub.liteflow.core.NodeComponent;
import com.yomahub.liteflow.lifecycle.PostProcessNodeExecuteLifeCycle;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;

/**
 * node 级指标采集（执行次数/耗时/在途/错误）
 *
 * @author Bryan.Zhang
 */
public class NodeMetricsLifeCycle implements PostProcessNodeExecuteLifeCycle {

    private final MeterRegistry registry;

    /** 每线程在途样本栈（仅用于 active LongTaskTimer，LIFO） */
    private static final ThreadLocal<Deque<LongTaskTimer.Sample>> ACTIVE_SAMPLES =
            ThreadLocal.withInitial(ArrayDeque::new);

    public NodeMetricsLifeCycle(MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void postProcessBeforeNodeExecute(NodeComponent cmp) {
        LongTaskTimer.Sample activeSample = LongTaskTimer.builder("liteflow.node.active")
                .tag("node", nodeId(cmp))
                .register(registry)
                .start();
        ACTIVE_SAMPLES.get().push(activeSample);
    }

    @Override
    public void postProcessAfterNodeExecute(NodeComponent cmp, long timeSpent, Exception e) {
        Deque<LongTaskTimer.Sample> stack = ACTIVE_SAMPLES.get();
        LongTaskTimer.Sample activeSample = stack.poll();
        try {
            String node = nodeId(cmp);
            String type = (cmp.getType() == null) ? "UNKNOWN" : cmp.getType().name();
            String status = (e == null) ? "success" : "failed";

            Timer.builder("liteflow.node.executions")
                    .tag("node", node)
                    .tag("type", type)
                    .tag("status", status)
                    .register(registry)
                    .record(timeSpent, TimeUnit.MILLISECONDS);

            if (activeSample != null) {
                activeSample.stop();
            }

            if (e != null) {
                registry.counter("liteflow.node.errors",
                        "node", node,
                        "exception", e.getClass().getSimpleName()).increment();
            }
        } finally {
            if (stack.isEmpty()) {
                ACTIVE_SAMPLES.remove();
            }
        }
    }

    private static String nodeId(NodeComponent cmp) {
        String id = cmp.getNodeId();
        return (id == null) ? "unknown" : id;
    }
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `mvn test -pl liteflow-metrics -Dtest=NodeMetricsLifeCycleTest -DskipTests=false`
Expected: PASS（2 个方法）。

- [ ] **Step 5: Commit**

```bash
git add liteflow-metrics/src/main/java/com/yomahub/liteflow/metrics/NodeMetricsLifeCycle.java \
        liteflow-metrics/src/test/java/com/yomahub/liteflow/metrics/NodeMetricsLifeCycleTest.java
git commit -m "feat(metrics): 新增 NodeMetricsLifeCycle node 级指标采集"
```

---

## Task 5: `LiteflowMetaView` —— 结构 + 指标快照取数

**Files:**
- Create: `liteflow-metrics/src/main/java/com/yomahub/liteflow/metrics/LiteflowMetaView.java`
- Test: `liteflow-metrics/src/test/java/com/yomahub/liteflow/metrics/LiteflowMetaViewTest.java`

**Interfaces:**
- Consumes: `FlowBus.getChainMap()`（`Map<String,Chain>`）、`FlowBus.getNodeMap()`（`Map<String,Node>`）、`Chain.getChainId()/getEl()/getNamespace()/getElMd5()`、`Node.getId()/getName()/getType()/getClazz()/getLanguage()`、`NodeTypeEnum.isScript()`、`MeterRegistry`（可空）。
- Produces: `class LiteflowMetaView`，构造 `LiteflowMetaView(MeterRegistry registryOrNull)`，方法：
  - `Map<String,Object> overview()`
  - `List<Map<String,Object>> chains()`
  - `Map<String,Object> chain(String chainId)`（不存在返回含 `error` 键的 Map）
  - `List<Map<String,Object>> nodes()`
  - `Map<String,Object> node(String nodeId)`

- [ ] **Step 1: 确认 Node 取数方法名**

Run: `grep -nE "public String (getId|getName|getClazz|getLanguage)\(|public NodeTypeEnum getType\(" liteflow-core/src/main/java/com/yomahub/liteflow/flow/element/Node.java`
Expected: 看到 `getId`、`getName`、`getType`、`getClazz`、`getLanguage` 的签名；若某方法名不同，按实际名修正下面 Step 4 代码。

- [ ] **Step 2: 写失败测试**

Create `liteflow-metrics/src/test/java/com/yomahub/liteflow/metrics/LiteflowMetaViewTest.java`:

```java
package com.yomahub.liteflow.metrics;

import com.yomahub.liteflow.flow.FlowBus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class LiteflowMetaViewTest {

    @Test
    public void testOverviewAndChains() {
        FlowBus.addChain("viewChain");

        LiteflowMetaView view = new LiteflowMetaView(null);

        Map<String, Object> overview = view.overview();
        Assertions.assertTrue(((Number) overview.get("chainsRegistered")).intValue() >= 1);
        Assertions.assertTrue(overview.containsKey("nodesRegistered"));

        List<Map<String, Object>> chains = view.chains();
        boolean found = chains.stream().anyMatch(c -> "viewChain".equals(c.get("chainId")));
        Assertions.assertTrue(found);
    }

    @Test
    public void testUnknownChainReturnsError() {
        LiteflowMetaView view = new LiteflowMetaView(null);
        Map<String, Object> result = view.chain("__not_exist__");
        Assertions.assertTrue(result.containsKey("error"));
    }
}
```

- [ ] **Step 3: 跑测试确认失败**

Run: `mvn test -pl liteflow-metrics -Dtest=LiteflowMetaViewTest -DskipTests=false`
Expected: 编译失败，`cannot find symbol: class LiteflowMetaView`。

- [ ] **Step 4: 实现 `LiteflowMetaView`**

Create `liteflow-metrics/src/main/java/com/yomahub/liteflow/metrics/LiteflowMetaView.java`:

```java
package com.yomahub.liteflow.metrics;

import com.yomahub.liteflow.flow.FlowBus;
import com.yomahub.liteflow.flow.element.Chain;
import com.yomahub.liteflow.flow.element.Node;
import com.yomahub.liteflow.meta.LiteflowMetaOperator;
import com.yomahub.liteflow.slot.DataBus;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.search.Search;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 结构检视 + 指标快照取数（纯 POJO，供 actuator 端点委托）
 *
 * @author Bryan.Zhang
 */
public class LiteflowMetaView {

    /** 可空：无 Micrometer 时仅返回结构信息 */
    private final MeterRegistry registry;

    public LiteflowMetaView(MeterRegistry registry) {
        this.registry = registry;
    }

    public Map<String, Object> overview() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("chainsRegistered", FlowBus.getChainMap().size());
        m.put("nodesRegistered", FlowBus.getNodeMap().size());
        m.put("slotOccupied", DataBus.OCCUPY_COUNT.get());
        m.put("chainIds", new ArrayList<>(FlowBus.getChainMap().keySet()));
        m.put("nodeIds", new ArrayList<>(FlowBus.getNodeMap().keySet()));
        return m;
    }

    public List<Map<String, Object>> chains() {
        List<Map<String, Object>> list = new ArrayList<>();
        for (Chain chain : FlowBus.getChainMap().values()) {
            list.add(chainBrief(chain));
        }
        return list;
    }

    public Map<String, Object> chain(String chainId) {
        Chain chain = FlowBus.getChainMap().get(chainId);
        if (chain == null) {
            return error("chain not found: " + chainId);
        }
        Map<String, Object> m = chainBrief(chain);
        m.put("metrics", chainMetrics(chainId));
        return m;
    }

    public List<Map<String, Object>> nodes() {
        List<Map<String, Object>> list = new ArrayList<>();
        for (Node node : FlowBus.getNodeMap().values()) {
            list.add(nodeBrief(node));
        }
        return list;
    }

    public Map<String, Object> node(String nodeId) {
        Node node = FlowBus.getNodeMap().get(nodeId);
        if (node == null) {
            return error("node not found: " + nodeId);
        }
        Map<String, Object> m = nodeBrief(node);
        m.put("metrics", nodeMetrics(nodeId));
        List<String> inChains = new ArrayList<>();
        for (Chain c : LiteflowMetaOperator.getChainsContainsNodeId(nodeId)) {
            inChains.add(c.getChainId());
        }
        m.put("inChains", inChains);
        return m;
    }

    private Map<String, Object> chainBrief(Chain chain) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("chainId", chain.getChainId());
        m.put("namespace", chain.getNamespace());
        m.put("el", chain.getEl());
        m.put("elMd5", chain.getElMd5());
        return m;
    }

    private Map<String, Object> nodeBrief(Node node) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("nodeId", node.getId());
        m.put("name", node.getName());
        m.put("type", node.getType() == null ? null : node.getType().name());
        m.put("script", node.getType() != null && node.getType().isScript());
        m.put("clazz", node.getClazz());
        m.put("language", node.getLanguage());
        return m;
    }

    private Map<String, Object> chainMetrics(String chainId) {
        if (registry == null) {
            return null;
        }
        return timerSnapshot(Search.in(registry).name("liteflow.chain.executions").tag("chain", chainId).timers());
    }

    private Map<String, Object> nodeMetrics(String nodeId) {
        if (registry == null) {
            return null;
        }
        return timerSnapshot(Search.in(registry).name("liteflow.node.executions").tag("node", nodeId).timers());
    }

    /** 把同名（不同 status）的多个 Timer 汇总成一个快照 */
    private Map<String, Object> timerSnapshot(java.util.Collection<Timer> timers) {
        long count = 0;
        long failed = 0;
        double totalMs = 0;
        double maxMs = 0;
        for (Timer t : timers) {
            long c = t.count();
            count += c;
            totalMs += t.totalTime(TimeUnit.MILLISECONDS);
            maxMs = Math.max(maxMs, t.max(TimeUnit.MILLISECONDS));
            if ("failed".equals(t.getId().getTag("status"))) {
                failed += c;
            }
        }
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("count", count);
        m.put("failed", failed);
        m.put("errorRate", count == 0 ? 0.0 : (double) failed / count);
        m.put("meanMs", count == 0 ? 0.0 : totalMs / count);
        m.put("maxMs", maxMs);
        return m;
    }

    private Map<String, Object> error(String msg) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("error", msg);
        return m;
    }
}
```

- [ ] **Step 5: 跑测试确认通过**

Run: `mvn test -pl liteflow-metrics -Dtest=LiteflowMetaViewTest -DskipTests=false`
Expected: PASS（2 个方法）。若 Step 1 发现 Node 方法名不同，先改 `nodeBrief`。

- [ ] **Step 6: 整模块回归**

Run: `mvn test -pl liteflow-metrics -DskipTests=false`
Expected: 全 PASS（4 个测试类）。

- [ ] **Step 7: Commit**

```bash
git add liteflow-metrics/src/main/java/com/yomahub/liteflow/metrics/LiteflowMetaView.java \
        liteflow-metrics/src/test/java/com/yomahub/liteflow/metrics/LiteflowMetaViewTest.java
git commit -m "feat(metrics): 新增 LiteflowMetaView 结构与指标快照取数"
```

---

## Task 6: Boot2/3 starter —— AutoConfiguration + `@Endpoint`

**Files:**
- Modify: `liteflow-spring-boot-starter/pom.xml`
- Create: `liteflow-spring-boot-starter/src/main/java/com/yomahub/liteflow/springboot/metrics/LiteflowMetricsAutoConfiguration.java`
- Create: `liteflow-spring-boot-starter/src/main/java/com/yomahub/liteflow/springboot/metrics/LiteflowEndpoint.java`
- Modify: `liteflow-spring-boot-starter/src/main/resources/META-INF/spring.factories`
- Modify: `liteflow-spring-boot-starter/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`
- Test: `liteflow-testcase-el/liteflow-testcase-el-springboot/src/test/java/com/yomahub/liteflow/test/metrics/MetricsEndpointSpringbootTest.java`（+ resources）

**Interfaces:**
- Consumes: `LiteflowMeterBinder`、`ChainMetricsLifeCycle`、`NodeMetricsLifeCycle`、`LiteflowMetaView`（Task 2-5）、`LiteflowConfig`（Spring 已有 bean）、`io.micrometer.core.instrument.MeterRegistry`（actuator 提供）。
- Produces: bean `liteflowMetaView`、actuator endpoint id `liteflow`、指标 `liteflow.*` 出现在 `MeterRegistry`。

- [ ] **Step 1: starter pom 加 optional 依赖**

Modify `liteflow-spring-boot-starter/pom.xml`：在 `<dependencies>` 内、`spring-boot-configuration-processor` 之后追加：

```xml
        <dependency>
            <groupId>com.yomahub</groupId>
            <artifactId>liteflow-metrics</artifactId>
            <version>${revision}</version>
        </dependency>
        <dependency>
            <groupId>io.micrometer</groupId>
            <artifactId>micrometer-core</artifactId>
            <optional>true</optional>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-actuator</artifactId>
            <optional>true</optional>
        </dependency>
```

- [ ] **Step 2: 写失败测试（端点 + 指标）**

Create `liteflow-testcase-el/liteflow-testcase-el-springboot/src/test/resources/metrics/flow.el.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE flow PUBLIC  "liteflow" "liteflow.dtd">
<flow>
    <chain name="mChain">
        THEN(a, b);
    </chain>
</flow>
```

Create `liteflow-testcase-el/liteflow-testcase-el-springboot/src/test/resources/metrics/application.properties`:

```properties
liteflow.rule-source=metrics/flow.el.xml
management.endpoints.web.exposure.include=liteflow,metrics,prometheus
management.endpoint.liteflow.enabled=true
```

Create `liteflow-testcase-el/liteflow-testcase-el-springboot/src/test/java/com/yomahub/liteflow/test/metrics/cmp/MaCmp.java`:

```java
package com.yomahub.liteflow.test.metrics.cmp;

import com.yomahub.liteflow.core.NodeComponent;
import org.springframework.stereotype.Component;

@Component("a")
public class MaCmp extends NodeComponent {
    @Override
    public void process() { }
}
```

Create `.../metrics/cmp/MbCmp.java`:

```java
package com.yomahub.liteflow.test.metrics.cmp;

import com.yomahub.liteflow.core.NodeComponent;
import org.springframework.stereotype.Component;

@Component("b")
public class MbCmp extends NodeComponent {
    @Override
    public void process() { }
}
```

Create `.../metrics/MetricsEndpointSpringbootTest.java`:

```java
package com.yomahub.liteflow.test.metrics;

import com.yomahub.liteflow.core.FlowExecutor;
import com.yomahub.liteflow.flow.LiteflowResponse;
import com.yomahub.liteflow.metrics.LiteflowMetaView;
import com.yomahub.liteflow.test.BaseTest;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.TestPropertySource;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

@TestPropertySource(value = "classpath:/metrics/application.properties")
@SpringBootTest(classes = MetricsEndpointSpringbootTest.class)
@EnableAutoConfiguration
@ComponentScan({ "com.yomahub.liteflow.test.metrics.cmp" })
public class MetricsEndpointSpringbootTest extends BaseTest {

    @Resource
    private FlowExecutor flowExecutor;

    @Resource
    private MeterRegistry meterRegistry;

    @Resource
    private LiteflowMetaView liteflowMetaView;

    @Test
    public void testChainAndNodeMetricsRecorded() {
        LiteflowResponse response = flowExecutor.execute2Resp("mChain", "arg");
        Assertions.assertTrue(response.isSuccess());

        Assertions.assertEquals(1,
                meterRegistry.get("liteflow.chain.executions")
                        .tags("chain", "mChain", "status", "success").timer().count());
        Assertions.assertEquals(1,
                meterRegistry.get("liteflow.node.executions")
                        .tags("node", "a", "status", "success").timer().count());
        Assertions.assertNotNull(meterRegistry.get("liteflow.slot.occupied").gauge());
    }

    @Test
    public void testMetaViewStructure() {
        List<Map<String, Object>> chains = liteflowMetaView.chains();
        boolean found = chains.stream().anyMatch(c -> "mChain".equals(c.get("chainId")));
        Assertions.assertTrue(found);
    }
}
```

- [ ] **Step 3: 跑测试确认失败**

Run: `mvn test -pl liteflow-testcase-el/liteflow-testcase-el-springboot -Dtest=MetricsEndpointSpringbootTest -DskipTests=false`
Expected: 失败 —— `liteflowMetaView` / `liteflow.chain.executions` 不存在（无装配 + testcase 模块缺 actuator）。

- [ ] **Step 4: testcase-el-springboot 加 actuator 测试依赖**

Run: `grep -nE "spring-boot-starter-actuator" liteflow-testcase-el/liteflow-testcase-el-springboot/pom.xml || echo MISSING`
若 MISSING，Modify `liteflow-testcase-el/liteflow-testcase-el-springboot/pom.xml` 在 `<dependencies>` 加：

```xml
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-actuator</artifactId>
            <scope>test</scope>
        </dependency>
```

- [ ] **Step 5: 实现端点类**

Create `liteflow-spring-boot-starter/src/main/java/com/yomahub/liteflow/springboot/metrics/LiteflowEndpoint.java`:

```java
package com.yomahub.liteflow.springboot.metrics;

import com.yomahub.liteflow.metrics.LiteflowMetaView;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.Selector;

import java.util.List;
import java.util.Map;

/**
 * LiteFlow 结构检视 actuator 端点：/actuator/liteflow
 *
 * @author Bryan.Zhang
 */
@Endpoint(id = "liteflow")
public class LiteflowEndpoint {

    private final LiteflowMetaView metaView;

    public LiteflowEndpoint(LiteflowMetaView metaView) {
        this.metaView = metaView;
    }

    @ReadOperation
    public Map<String, Object> overview() {
        return metaView.overview();
    }

    @ReadOperation
    public Object chains(@Selector String level) {
        // /actuator/liteflow/chains 或 /actuator/liteflow/nodes
        if ("chains".equals(level)) {
            return metaView.chains();
        }
        if ("nodes".equals(level)) {
            return metaView.nodes();
        }
        return metaView.error("unknown selector: " + level);
    }

    @ReadOperation
    public Map<String, Object> detail(@Selector String level, @Selector String id) {
        // /actuator/liteflow/chains/{id} 或 /actuator/liteflow/nodes/{id}
        if ("chains".equals(level)) {
            return metaView.chain(id);
        }
        if ("nodes".equals(level)) {
            return metaView.node(id);
        }
        return metaView.error("unknown selector: " + level);
    }
}
```

> 注：`metaView.error(...)` 需要是 public。Modify `LiteflowMetaView.error` 方法可见性由 `private` 改为 `public`（Task 5 中定义），并在 Task 5 已提交的基础上随本任务一并修改提交。

- [ ] **Step 6: 实现 AutoConfiguration**

Create `liteflow-spring-boot-starter/src/main/java/com/yomahub/liteflow/springboot/metrics/LiteflowMetricsAutoConfiguration.java`:

```java
package com.yomahub.liteflow.springboot.metrics;

import com.yomahub.liteflow.metrics.ChainMetricsLifeCycle;
import com.yomahub.liteflow.metrics.LiteflowMetaView;
import com.yomahub.liteflow.metrics.LiteflowMeterBinder;
import com.yomahub.liteflow.metrics.NodeMetricsLifeCycle;
import com.yomahub.liteflow.property.LiteflowConfig;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * LiteFlow 指标与结构端点装配
 *
 * @author Bryan.Zhang
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(MeterRegistry.class)
@ConditionalOnProperty(prefix = "liteflow.metrics", name = "enabled", havingValue = "true", matchIfMissing = true)
public class LiteflowMetricsAutoConfiguration {

    @Bean
    @ConditionalOnBean({ MeterRegistry.class, LiteflowConfig.class })
    @ConditionalOnMissingBean
    public LiteflowMeterBinder liteflowMeterBinder(LiteflowConfig liteflowConfig) {
        return new LiteflowMeterBinder(liteflowConfig);
    }

    @Bean
    @ConditionalOnBean(MeterRegistry.class)
    @ConditionalOnMissingBean
    public ChainMetricsLifeCycle chainMetricsLifeCycle(MeterRegistry meterRegistry) {
        return new ChainMetricsLifeCycle(meterRegistry);
    }

    @Bean
    @ConditionalOnBean(MeterRegistry.class)
    @ConditionalOnMissingBean
    public NodeMetricsLifeCycle nodeMetricsLifeCycle(MeterRegistry meterRegistry) {
        return new NodeMetricsLifeCycle(meterRegistry);
    }

    @Bean
    @ConditionalOnMissingBean
    public LiteflowMetaView liteflowMetaView(ObjectProvider<MeterRegistry> meterRegistry) {
        return new LiteflowMetaView(meterRegistry.getIfAvailable());
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(Endpoint.class)
    static class LiteflowEndpointConfiguration {
        @Bean
        @ConditionalOnMissingBean
        public LiteflowEndpoint liteflowEndpoint(LiteflowMetaView metaView) {
            return new LiteflowEndpoint(metaView);
        }
    }
}
```

- [ ] **Step 7: 注册 AutoConfiguration**

Modify `liteflow-spring-boot-starter/src/main/resources/META-INF/spring.factories`，在末尾 `LiteflowMainAutoConfiguration` 后追加 `,\`+换行+：

```
  com.yomahub.liteflow.springboot.metrics.LiteflowMetricsAutoConfiguration
```

即整体变为：

```
org.springframework.boot.autoconfigure.EnableAutoConfiguration=\
  com.yomahub.liteflow.springboot.config.LiteflowPropertyAutoConfiguration,\
  com.yomahub.liteflow.springboot.config.LiteflowMainAutoConfiguration,\
  com.yomahub.liteflow.springboot.metrics.LiteflowMetricsAutoConfiguration
```

Modify `liteflow-spring-boot-starter/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`，末尾追加一行：

```
com.yomahub.liteflow.springboot.metrics.LiteflowMetricsAutoConfiguration
```

- [ ] **Step 8: 把 `LiteflowMetaView.error` 改为 public 并重装 metrics**

Modify `liteflow-metrics/src/main/java/com/yomahub/liteflow/metrics/LiteflowMetaView.java`：把 `private Map<String, Object> error(String msg)` 改为 `public Map<String, Object> error(String msg)`。

Run: `mvn install -pl liteflow-core,liteflow-metrics -DskipTests`
Expected: BUILD SUCCESS（让 starter 能引用最新 metrics）。

- [ ] **Step 9: 跑测试确认通过**

Run: `mvn install -pl liteflow-spring-boot-starter -DskipTests && mvn test -pl liteflow-testcase-el/liteflow-testcase-el-springboot -Dtest=MetricsEndpointSpringbootTest -DskipTests=false`
Expected: PASS（2 个方法）。

- [ ] **Step 10: Commit**

```bash
git add liteflow-spring-boot-starter/pom.xml \
        liteflow-spring-boot-starter/src/main/java/com/yomahub/liteflow/springboot/metrics \
        liteflow-spring-boot-starter/src/main/resources/META-INF/spring.factories \
        liteflow-spring-boot-starter/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports \
        liteflow-metrics/src/main/java/com/yomahub/liteflow/metrics/LiteflowMetaView.java \
        liteflow-testcase-el/liteflow-testcase-el-springboot/pom.xml \
        liteflow-testcase-el/liteflow-testcase-el-springboot/src/test/java/com/yomahub/liteflow/test/metrics \
        liteflow-testcase-el/liteflow-testcase-el-springboot/src/test/resources/metrics
git commit -m "feat(starter): Boot2/3 指标装配与 /actuator/liteflow 结构端点"
```

---

## Task 7: Boot4 starter —— AutoConfiguration + `@Endpoint`

**Files:**
- Modify: `liteflow-spring-boot4-starter/pom.xml`
- Create: `liteflow-spring-boot4-starter/src/main/java/com/yomahub/liteflow/springboot4/metrics/LiteflowMetricsAutoConfiguration.java`
- Create: `liteflow-spring-boot4-starter/src/main/java/com/yomahub/liteflow/springboot4/metrics/LiteflowEndpoint.java`
- Modify: `liteflow-spring-boot4-starter/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`
- Test: `liteflow-testcase-el/liteflow-testcase-el-springboot4/src/test/java/com/yomahub/liteflow/test/metrics/...`

> 仅在 JDK 17+ 构建（`compile-17+` profile）。本任务命令需在 JDK 17+ 环境执行（本仓库开发 JDK 为 21）。

**Interfaces:**
- 与 Task 6 相同，包名 `com.yomahub.liteflow.springboot4.metrics`。

- [ ] **Step 1: starter pom 加 optional 依赖**

Modify `liteflow-spring-boot4-starter/pom.xml`：在 `<dependencies>` 内追加（同 Task 6 Step 1 的三段依赖）：

```xml
        <dependency>
            <groupId>com.yomahub</groupId>
            <artifactId>liteflow-metrics</artifactId>
            <version>${revision}</version>
        </dependency>
        <dependency>
            <groupId>io.micrometer</groupId>
            <artifactId>micrometer-core</artifactId>
            <optional>true</optional>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-actuator</artifactId>
            <optional>true</optional>
        </dependency>
```

- [ ] **Step 2: 写失败测试**

Create `liteflow-testcase-el/liteflow-testcase-el-springboot4/src/test/resources/metrics/flow.el.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE flow PUBLIC  "liteflow" "liteflow.dtd">
<flow>
    <chain name="mChain">
        THEN(a, b);
    </chain>
</flow>
```

Create `liteflow-testcase-el/liteflow-testcase-el-springboot4/src/test/resources/metrics/application.properties`:

```properties
liteflow.rule-source=metrics/flow.el.xml
management.endpoints.web.exposure.include=liteflow,metrics,prometheus
management.endpoint.liteflow.enabled=true
```

Create `liteflow-testcase-el/liteflow-testcase-el-springboot4/src/test/java/com/yomahub/liteflow/test/metrics/cmp/MaCmp.java`:

```java
package com.yomahub.liteflow.test.metrics.cmp;

import com.yomahub.liteflow.core.NodeComponent;
import org.springframework.stereotype.Component;

@Component("a")
public class MaCmp extends NodeComponent {
    @Override
    public void process() { }
}
```

Create `.../metrics/cmp/MbCmp.java`:

```java
package com.yomahub.liteflow.test.metrics.cmp;

import com.yomahub.liteflow.core.NodeComponent;
import org.springframework.stereotype.Component;

@Component("b")
public class MbCmp extends NodeComponent {
    @Override
    public void process() { }
}
```

Create `.../metrics/MetricsEndpointSpringboot4Test.java`：

```java
package com.yomahub.liteflow.test.metrics;

import com.yomahub.liteflow.core.FlowExecutor;
import com.yomahub.liteflow.flow.LiteflowResponse;
import com.yomahub.liteflow.metrics.LiteflowMetaView;
import com.yomahub.liteflow.test.BaseTest;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.TestPropertySource;

import jakarta.annotation.Resource;
import java.util.List;
import java.util.Map;

@TestPropertySource(value = "classpath:/metrics/application.properties")
@SpringBootTest(classes = MetricsEndpointSpringboot4Test.class)
@EnableAutoConfiguration
@ComponentScan({ "com.yomahub.liteflow.test.metrics.cmp" })
public class MetricsEndpointSpringboot4Test extends BaseTest {

    @Resource
    private FlowExecutor flowExecutor;

    @Resource
    private MeterRegistry meterRegistry;

    @Resource
    private LiteflowMetaView liteflowMetaView;

    @Test
    public void testChainAndNodeMetricsRecorded() {
        LiteflowResponse response = flowExecutor.execute2Resp("mChain", "arg");
        Assertions.assertTrue(response.isSuccess());

        Assertions.assertEquals(1,
                meterRegistry.get("liteflow.chain.executions")
                        .tags("chain", "mChain", "status", "success").timer().count());
        Assertions.assertEquals(1,
                meterRegistry.get("liteflow.node.executions")
                        .tags("node", "a", "status", "success").timer().count());
    }

    @Test
    public void testMetaViewStructure() {
        List<Map<String, Object>> chains = liteflowMetaView.chains();
        boolean found = chains.stream().anyMatch(c -> "mChain".equals(c.get("chainId")));
        Assertions.assertTrue(found);
    }
}
```

> 注意 Boot4 用 `jakarta.annotation.Resource`（非 `javax`）。若该 testcase 模块已有约定的注入方式，按既有测试的 import 习惯对齐。

- [ ] **Step 3: 跑测试确认失败**

Run: `mvn test -pl liteflow-testcase-el/liteflow-testcase-el-springboot4 -Dtest=MetricsEndpointSpringboot4Test -DskipTests=false`
Expected: 失败（无装配 / 缺 actuator）。

- [ ] **Step 4: testcase-el-springboot4 加 actuator 测试依赖**

Run: `grep -nE "spring-boot-starter-actuator" liteflow-testcase-el/liteflow-testcase-el-springboot4/pom.xml || echo MISSING`
若 MISSING，Modify 该 pom 在 `<dependencies>` 加：

```xml
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-actuator</artifactId>
            <scope>test</scope>
        </dependency>
```

- [ ] **Step 5: 实现端点类**

Create `liteflow-spring-boot4-starter/src/main/java/com/yomahub/liteflow/springboot4/metrics/LiteflowEndpoint.java`（与 Task 6 Step 5 完全相同，仅把包名首行改为 `package com.yomahub.liteflow.springboot4.metrics;`）：

```java
package com.yomahub.liteflow.springboot4.metrics;

import com.yomahub.liteflow.metrics.LiteflowMetaView;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.Selector;

import java.util.Map;

@Endpoint(id = "liteflow")
public class LiteflowEndpoint {

    private final LiteflowMetaView metaView;

    public LiteflowEndpoint(LiteflowMetaView metaView) {
        this.metaView = metaView;
    }

    @ReadOperation
    public Map<String, Object> overview() {
        return metaView.overview();
    }

    @ReadOperation
    public Object chains(@Selector String level) {
        if ("chains".equals(level)) {
            return metaView.chains();
        }
        if ("nodes".equals(level)) {
            return metaView.nodes();
        }
        return metaView.error("unknown selector: " + level);
    }

    @ReadOperation
    public Map<String, Object> detail(@Selector String level, @Selector String id) {
        if ("chains".equals(level)) {
            return metaView.chain(id);
        }
        if ("nodes".equals(level)) {
            return metaView.node(id);
        }
        return metaView.error("unknown selector: " + level);
    }
}
```

- [ ] **Step 6: 实现 AutoConfiguration（Boot4 用 `@AutoConfiguration`）**

Create `liteflow-spring-boot4-starter/src/main/java/com/yomahub/liteflow/springboot4/metrics/LiteflowMetricsAutoConfiguration.java`:

```java
package com.yomahub.liteflow.springboot4.metrics;

import com.yomahub.liteflow.metrics.ChainMetricsLifeCycle;
import com.yomahub.liteflow.metrics.LiteflowMetaView;
import com.yomahub.liteflow.metrics.LiteflowMeterBinder;
import com.yomahub.liteflow.metrics.NodeMetricsLifeCycle;
import com.yomahub.liteflow.property.LiteflowConfig;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@AutoConfiguration
@ConditionalOnClass(MeterRegistry.class)
@ConditionalOnProperty(prefix = "liteflow.metrics", name = "enabled", havingValue = "true", matchIfMissing = true)
public class LiteflowMetricsAutoConfiguration {

    @Bean
    @ConditionalOnBean({ MeterRegistry.class, LiteflowConfig.class })
    @ConditionalOnMissingBean
    public LiteflowMeterBinder liteflowMeterBinder(LiteflowConfig liteflowConfig) {
        return new LiteflowMeterBinder(liteflowConfig);
    }

    @Bean
    @ConditionalOnBean(MeterRegistry.class)
    @ConditionalOnMissingBean
    public ChainMetricsLifeCycle chainMetricsLifeCycle(MeterRegistry meterRegistry) {
        return new ChainMetricsLifeCycle(meterRegistry);
    }

    @Bean
    @ConditionalOnBean(MeterRegistry.class)
    @ConditionalOnMissingBean
    public NodeMetricsLifeCycle nodeMetricsLifeCycle(MeterRegistry meterRegistry) {
        return new NodeMetricsLifeCycle(meterRegistry);
    }

    @Bean
    @ConditionalOnMissingBean
    public LiteflowMetaView liteflowMetaView(ObjectProvider<MeterRegistry> meterRegistry) {
        return new LiteflowMetaView(meterRegistry.getIfAvailable());
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(Endpoint.class)
    static class LiteflowEndpointConfiguration {
        @Bean
        @ConditionalOnMissingBean
        public LiteflowEndpoint liteflowEndpoint(LiteflowMetaView metaView) {
            return new LiteflowEndpoint(metaView);
        }
    }
}
```

- [ ] **Step 7: 注册 AutoConfiguration**

Modify `liteflow-spring-boot4-starter/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`，末尾追加：

```
com.yomahub.liteflow.springboot4.metrics.LiteflowMetricsAutoConfiguration
```

- [ ] **Step 8: 跑测试确认通过**

Run: `mvn install -pl liteflow-core,liteflow-metrics,liteflow-spring-boot4-starter -DskipTests && mvn test -pl liteflow-testcase-el/liteflow-testcase-el-springboot4 -Dtest=MetricsEndpointSpringboot4Test -DskipTests=false`
Expected: PASS（2 个方法）。

- [ ] **Step 9: Commit**

```bash
git add liteflow-spring-boot4-starter/pom.xml \
        liteflow-spring-boot4-starter/src/main/java/com/yomahub/liteflow/springboot4/metrics \
        liteflow-spring-boot4-starter/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports \
        liteflow-testcase-el/liteflow-testcase-el-springboot4/pom.xml \
        liteflow-testcase-el/liteflow-testcase-el-springboot4/src/test/java/com/yomahub/liteflow/test/metrics \
        liteflow-testcase-el/liteflow-testcase-el-springboot4/src/test/resources/metrics
git commit -m "feat(starter): Boot4 指标装配与 /actuator/liteflow 结构端点"
```

---

## Task 8: 使用文档

**Files:**
- Create: `docs/liteflow-metrics-guide.md`

- [ ] **Step 1: 写文档**

Create `docs/liteflow-metrics-guide.md`，覆盖：

1. **引入依赖**：Boot 项目加 `liteflow-spring-boot-starter`(或 boot4) + `spring-boot-starter-actuator` + 一个 registry（如 `micrometer-registry-prometheus`）。说明"加 `liteflow-metrics` 依赖即生效"。
2. **开关**：`liteflow.metrics.enabled`（默认 true）；`management.endpoints.web.exposure.include=liteflow,prometheus,metrics`。
3. **指标目录**：从 spec §5 复制 chain/node/全局三张表（指标名、类型、tag、含义）。
4. **结构端点**：`/actuator/liteflow` 五条路由及示例 JSON。
5. **分位/直方图**：用 `management.metrics.distribution.percentiles[liteflow.chain.executions]=0.95,0.99` 等标准配置。
6. **常用 PromQL 与告警**：QPS、平均耗时、错误率、slot 饱和度。
7. **职责归属与性能**：摘 spec §5.4、§8 一段，说明 LiteFlow 只上报、聚合归 Micrometer/Prometheus，开销 <1%。

- [ ] **Step 2: 全量回归**

Run: `mvn test -pl liteflow-metrics -DskipTests=false && mvn test -pl liteflow-testcase-el/liteflow-testcase-el-springboot -Dtest=NodeExecuteLifeCycleSpringbootTest,MetricsEndpointSpringbootTest -DskipTests=false`
Expected: 全 PASS。

- [ ] **Step 3: Commit**

```bash
git add docs/liteflow-metrics-guide.md
git commit -m "docs: 新增 liteflow-metrics 使用指南"
```

---

## Self-Review 结论

- **Spec 覆盖**：§3 架构→Task1-7；§4 core 改动→Task1；§5.1 chain 指标→Task3；§5.2 node 指标→Task4；§5.3 全局 Gauge→Task2；§5.4 职责归属→文档 Task8；§6 配置→Task6/7 的 `@ConditionalOnProperty`；§7 结构端点→Task6/7 `LiteflowEndpoint`+Task5 `LiteflowMetaView`；§8 性能/基数→设计约束已体现在低基数 tag；§9 兼容性→Task6(Boot2/3)+Task7(Boot4)；§10 测试→各 Task 内置；§11 落地顺序→Task 编号一致。Phase 2(§5.5) 不实现，符合 spec。
- **类型/签名一致性**：`PostProcessNodeExecuteLifeCycle` 的 `postProcessBeforeNodeExecute(NodeComponent)` / `postProcessAfterNodeExecute(NodeComponent,long,Exception)` 在 Task1 定义、Task4 与 core 接入处一致；`LiteflowMetaView` 构造签名 `(MeterRegistry)` 在 Task5 定义、Task6/7 注入一致；`error(...)` 在 Task5 定义、Task6 Step8 提升为 public 后被端点调用。
- **已知校验点**：Task5 Step1 先核对 `Node` 取数方法名（`getId/getName/getType/getClazz/getLanguage`）；Task7 用 `jakarta.annotation.Resource`。
