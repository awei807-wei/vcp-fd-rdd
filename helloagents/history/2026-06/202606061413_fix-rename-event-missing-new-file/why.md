# 修复 rename 事件导致新下载文件搜不到的问题原因

## 背景

用户反馈：刚下载完的文件搜不到，或要等一段时间才能搜到。这是最高频的体感缺陷，直接影响索引器的可用性。

## 根因分析

下载器（浏览器、curl、wget、aria2 等）的写入模式是：
1. 写入临时文件（`.crdownload` / `.part` / `.tmp`）
2. 下载完成后 `rename()` 到最终文件名

inotify 把 rename 拆成两个独立事件：`RenameFrom` + `RenameTo`，通过 cookie 配对。

代码中存在两处缺陷：

### 缺陷一：跨批次 rename 配对导致延迟（stream.rs）

`PendingMoveMap` 用于跨 debounce 批次配对 `RenameFrom` / `RenameTo`。当两个事件落在不同批次时：

- 第一批：`RenameFrom` 进入 pending map，**从 raw_events 中被移除**，当前批次不写索引
- 第二批：`RenameTo` 到来时才配对成功，写入索引

延迟约等于一到两个 debounce 周期（50ms），系统繁忙时更长。但更严重的情况是：

### 缺陷二：孤立 RenameTo 被静默丢弃（stream.rs:merge_events_in_place）

当 `RenameFrom` 发生在 inotify watch 注册之前（watch 刚启动、目录刚被 add 进 tiered watcher 时），`RenameFrom` 事件根本不会到达管道。

此时只有孤立的 `RenameTo` 事件，路径只有一个。`merge_events_in_place` 处理单路径 rename 时，`paths.len() < 2`，走到 "普通事件" 分支，`kind` 被转换为 `EventType::Other` 或 `EventType::Modify`（取决于 `EventKind::into()` 实现），**最终文件名没有以 Create 语义写入索引，新文件对搜索不可见**。

### 缺陷三：merge 时 Rename 被后续 Modify 覆盖

同一批次内，如果 rename 配对成功后同路径又有 Modify 事件，`scratch.merged.insert` 后到覆盖先到，rename 语义变成 Modify，索引层可能把新文件当作已有文件的修改处理，在旧路径找不到记录时静默失败。

## 影响范围

- 所有通过 rename 写入的文件：下载器、编辑器保存、编译产物
- tiered watcher 模式下 watch 刚注册后的窗口期
- watch 重启 / 目录第一次被加入 ephemeral watch 时
