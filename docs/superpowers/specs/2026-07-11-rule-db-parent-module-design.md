# Rule-DB 独立父模块迁移设计

## 目标

将 `liteflow-rule-db-sql` 和 `liteflow-rule-db-redis` 从 `liteflow-rule-plugin`
中移出，归入根目录下新建的独立父模块 `liteflow-rule-db`。本次迁移只调整
源码目录和 Maven 聚合关系，不改变两个子模块的发布坐标、依赖关系、Java 包名
或运行行为。

## 目标目录结构

```text
liteflow-rule-db/
├── pom.xml
├── liteflow-rule-db-sql/
└── liteflow-rule-db-redis/
```

`liteflow-rule-db` 与 `liteflow-rule-plugin` 在仓库根目录下平级。前者只聚合
Rule-DB 的 SQL 和 Redis 实现，后者继续聚合原有的规则源插件。

## Maven 结构

- 根 `pom.xml` 的 `compile-8-to-16`、`compile-17+` 和 `release-on-8`
  profile 均新增 `liteflow-rule-db` 模块。
- `liteflow-rule-plugin/pom.xml` 删除 `liteflow-rule-db-sql` 和
  `liteflow-rule-db-redis` 两个子模块声明。
- 新增 `liteflow-rule-db/pom.xml`，继承根项目 `liteflow`，打包类型为 `pom`，
  并聚合两个 Rule-DB 子模块。
- 两个子模块的父项目改为 `liteflow-rule-db`，`relativePath` 继续指向
  `../pom.xml`。
- 两个子模块继续使用现有的 `com.yomahub` groupId、artifactId 和
  `${revision}` 版本，不影响下游依赖声明及发布制品名称。

## 文档迁移

仓库中所有指向 `liteflow-rule-plugin` 下原有两个 Rule-DB 子目录的内容均更新
为新目录，包括：

- 当前使用指南和仓库说明；
- 历史设计规格；
- 历史实施计划；
- 构建命令、文件清单和 Git 命令示例。

描述两个模块隶属于 `liteflow-rule-plugin` 的文字也同步改为隶属于独立的
`liteflow-rule-db` 父模块。仅提及 artifactId、依赖用法或功能名称的内容无需
修改。

## 验证标准

1. 仓库中不存在两个 Rule-DB 模块的旧目录路径引用。
2. Maven reactor 能识别新的父子模块关系，且不再从 `liteflow-rule-plugin`
   聚合两个 Rule-DB 模块。
3. `liteflow-rule-db-sql` 和 `liteflow-rule-db-redis` 均可通过新 reactor 路径
   完成构建。
4. 现有 Rule-DB SQL 和 Redis 测试模块仍能解析相同 artifactId 的依赖。
5. 除目录和父 POM 变化外，不产生 Java 源码或运行行为变更。
