# 动态内存优化（为什么做）

当前阶段 A（DocId + RoaringBitmap + PathArena）已让“静态加载快照 + no-watch/no-build”时 RSS 与 L2 估算基本贴合，说明索引结构本身已较紧凑。

动态场景（watcher 开启、事件风暴、触发 rebuild）下，RSS 仍会被“分配高水位/碎片 + 容器扩容 + 临时分配”拉高，并可能长期不回落。

本方案聚焦两个低风险、高收益的动态优化：

1) 分配器替换（mimalloc）
   - 目标：降低碎片与 RSS 高水位粘住概率，改善动态更新/重建后的回吐表现。

2) 路径进一步压缩（替代字符串池化的更有效手段）
   - 字符串 interning 对“完全相同字符串”收益有限，且会“只增不减”。
   - fd-rdd 的路径更高重复的是 root 前缀（如 $HOME），更适合做“相对 root 存储/前缀剥离”，直接减少 arena 的 bytes。

明确不做：

- fanotify：属于更大范围的 watcher 架构改造，留到后续阶段（与 ArcSwap / mmap 段式一起评估）。

