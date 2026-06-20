// TODO(architecture): Partition 定义已移至 rdd.rs，此文件仅保留兼容重导出。
// 可考虑直接删除此文件并在 mod.rs 中移除 `pub mod partition;`，
// 将所有 `use crate::core::partition::Partition` 改为 `use crate::core::rdd::Partition`。
pub use crate::core::rdd::Partition;
