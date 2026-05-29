#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// 编译期可见的分配器选择（用于回归测试与诊断输出）。
pub const ALLOCATOR_KIND: &str = if cfg!(feature = "mimalloc") {
    "mimalloc"
} else {
    "system"
};

pub mod clock;
pub mod config;
pub mod core;
pub mod diagnostics;
pub mod event;
pub mod fs_policy;
pub mod index;
pub mod io_governor;
pub mod query;
pub mod security;
pub mod sim;
pub mod stats;
pub mod storage;
pub mod util;
