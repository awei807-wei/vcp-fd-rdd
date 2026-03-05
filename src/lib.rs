#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// 编译期可见的分配器选择（用于回归测试与诊断输出）。
pub const ALLOCATOR_KIND: &str = if cfg!(feature = "mimalloc") {
    "mimalloc"
} else {
    "system"
};

pub mod core;
pub mod event;
pub mod index;
pub mod query;
pub mod stats;
pub mod storage;
