# M1-1 查询返回前 Top-K 验真需求

## 背景

路线基准已确认：热点 5 秒 + 全局有界最终一致 + 返回前验真。对用户而言，搜索结果的产品口径应从“索引可能过期”收敛为“返回的结果是真的，但候选集合可能暂时不全；不全的部分有可解释追平上界”。

## 现状问题

- 当前默认 `lazy_validation_enabled = true`，冷层/base 命中会先返回 `freshness = "unknown"` 且 `validated = false`，再由后台 worker 修复。这适合低功耗补偿，但不适合作为默认正确性路径。
- 同步冷层校验没有显式单查询预算。若宽泛查询命中大量 stale/deleted 候选，为填满 Top-K 可能执行过多 `stat`。
- 查询配置还没有 `query.max_verify_per_query`、`query.verify_timeout_ms` 和 `query.allow_sync_readdir` 这组对外可解释预算。

## 成功标准

- 默认配置下，冷层/base 返回给用户前必须经过同步 `stat` 验真；被删除路径不返回，mtime 或 identity 变化不作为 `StaleChecked` 返回。
- 单次查询同步验真受数量和时间双重预算限制，预算耗尽后返回已确认的前缀结果，不用同步 readdir 扩大候选集合。
- lazy validation 保留为显式低功耗/后台补偿能力，不再是默认正确性路径。
