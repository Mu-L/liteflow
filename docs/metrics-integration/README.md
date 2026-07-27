# LiteFlow 指标可视化最小栈

一份开箱即用的 Prometheus + Grafana 配置，用来把 LiteFlow 暴露的指标抓取、存储并画成仪表盘。

完整说明见 [`../liteflow-metrics-guide.md`](../liteflow-metrics-guide.md) 的「上手篇」。

## 前提

你的应用已经能访问到 Prometheus 出口（指南上手篇 Step 1~2）：

```bash
curl http://localhost:8080/actuator/prometheus | grep liteflow_
```

能看到 `liteflow_...` 开头的行就 OK。

## 启动

```bash
# 在本目录执行
docker compose up -d
```

- Grafana：<http://localhost:3000>（已开匿名访问，免登录）→ 仪表盘「LiteFlow 概览」已自动挂好
- Prometheus：<http://localhost:9090>（Status → Targets 可确认是否抓到你的应用）

## 改抓取目标

默认抓 `host.docker.internal:8080`。端口/地址不同就改 [`prometheus.yml`](./prometheus.yml) 里的 `targets`，然后 `docker compose restart prometheus`。

## 停止

```bash
docker compose down
```

## 目录说明

```
docker-compose.yml                         Prometheus + Grafana 两个容器
prometheus.yml                             抓取配置（改这里的 targets）
grafana/provisioning/datasources/          启动时自动挂好 Prometheus 数据源
grafana/provisioning/dashboards/           启动时自动导入仪表盘的 provider 配置
grafana/dashboards/liteflow-dashboard.json 仪表盘本体（也可单独导入到已有 Grafana）
```

> 全部为演示配置（匿名访问、弱口令、5s 抓取间隔），请勿直接用于生产。
