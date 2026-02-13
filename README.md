# fd-rdd: RDD-based Elastic File Indexer

> **"Everything" for Linux**, powered by Spark-core ideas in Rust.

`fd-rdd` 是一个高性能、事件驱动的 Linux 文件索引服务。它借鉴了 Apache Spark 的 **RDD (Resilient Distributed Datasets)** 思想，通过三级索引架构实现了毫秒级的搜索响应、极低的内存占用以及强大的容错恢复能力。

---

## 🚀 核心架构：三级索引漏斗 (Tiered Indexing)

`fd-rdd` 不仅仅是一个简单的文件名数据库，它通过三层过滤机制平衡了速度与完整性：

1.  **L1 (Hot Cache)**: 基于 `DashMap` 的并发哈希表。存储最近最常访问的搜索结果，响应时间 **< 1ms**。
2.  **L2 (Warm RDD)**: 核心层。将文件系统划分为多个 **RDD Partitions**。
    *   **血缘追踪 (Lineage)**: 记录文件变更事件流，支持在索引损坏时通过事件回溯实现弹性恢复。
    *   **窄依赖更新**: 当某个目录发生变动时，仅重算受影响的分区，而非全量扫描。
    *   **并行过滤**: 利用 `rayon` 实现多核并行检索，响应时间 **< 10ms**。
3.  **L3 (Cold Scan)**: 弹性兜底层。基于 `ignore` 库（fd 核心）进行实时扫描。
    *   **AQE (Adaptive Query Execution)**: 根据系统当前 CPU/IO 负载，动态调整扫描线程数和深度，确保不影响主业务性能。

---

## ✨ 关键特性

*   **实时感知**: 集成 `notify` 库，毫秒级捕获文件创建、删除、重命名（自动追踪路径移动）。
*   **VCP 深度集成**: 内置基于 `Axum` 的高性能 HTTP 服务，完美适配 VCP 插件调用。
*   **低资源占用**: 默认配置下内存占用极低，支持通过 Systemd 限制 CPU 权重。
*   **开发者友好**: 提供标准 JSON 接口与交互式 CLI 工具。

---

## 🛠️ 快速开始

### 1. 安装与编译

确保已安装 Rust 工具链，然后运行安装脚本：

```bash
# 使用自动化脚本安装到 ~/.vcp/bin
./scripts/install.sh

# 或者手动编译
cargo build --release
```

### 2. 启动服务

你可以直接运行二进制文件，或者将其注册为用户级服务：

```bash
# 直接启动 (默认监听 6060 端口)
./target/release/fd-rdd

# 使用 Systemd 管理 (推荐)
mkdir -p ~/.config/systemd/user/
cp scripts/fd-rdd.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now fd-rdd
```

### 3. 搜索与交互

**HTTP API:**
```bash
curl "http://127.0.0.1:6060/search?q=main.rs&limit=5"
```

**命令行工具:**
```bash
./scripts/fd-query.sh "your_keyword"
```

---

## 📊 接口文档

### `GET /search`
搜索文件索引。
*   **参数**: `q` (关键词), `limit` (可选，默认100)
*   **返回**: `[{"path": "/path/to/file", "score": 1.0}, ...]`

### `GET /status`
获取服务运行状态。
*   **返回**: `{"indexed_count": 120, "memory_usage": "12MB", "l1_hit_rate": "85%"}`

---

## 📂 项目结构

*   [`src/core/`](fd-rdd/src/core/): RDD 抽象、血缘追踪 (Lineage) 与自适应调度器 (AQE)。
*   [`src/index/`](fd-rdd/src/index/): 三级索引引擎（L1/L2/L3）的具体实现。
*   [`src/event/`](fd-rdd/src/event/): 基于 `notify` 的实时事件流处理。
*   [`src/query/`](fd-rdd/src/query/): Axum HTTP 服务与搜索路由。
*   [`scripts/`](fd-rdd/scripts/): 安装脚本、Systemd 配置及 CLI 客户端。

---

## 📜 许可证

MIT License © 2026 fd-rdd Contributors