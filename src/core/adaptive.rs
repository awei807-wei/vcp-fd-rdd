use std::sync::atomic::{AtomicUsize, Ordering};
use sysinfo::System;

/// 自适应调度器（类 Spark Adaptive Execution）
pub struct AdaptiveScheduler {
    target_parallelism: AtomicUsize,
    system: System,
}

impl Default for AdaptiveScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl AdaptiveScheduler {
    pub fn new() -> Self {
        let mut sys = System::new();
        sys.refresh_memory();

        Self {
            target_parallelism: AtomicUsize::new(num_cpus::get()),
            system: sys,
        }
    }

    /// 动态调整并行度
    pub fn adjust_parallelism(&mut self) -> usize {
        self.system.refresh_memory();

        let load = System::load_average().one;
        let cpu_count = num_cpus::get() as f64;
        let mem_free = self.system.available_memory();
        let total_mem = self.system.total_memory().max(1);
        let mem_pressure = 1.0 - (mem_free as f64 / total_mem as f64);

        // 计算目标并行度
        let new_parallelism = if load < cpu_count * 0.3 && mem_pressure < 0.5 {
            // 系统空闲：超线程激进
            (cpu_count * 2.0) as usize
        } else if load > cpu_count * 0.8 || mem_pressure > 0.8 {
            // 系统繁忙：保守降级
            (cpu_count * 0.5) as usize
        } else {
            // 正常负载：匹配核心数
            cpu_count as usize
        };

        let old = self
            .target_parallelism
            .swap(new_parallelism, Ordering::Relaxed);

        if old != new_parallelism {
            tracing::info!(
                "Adaptive parallelism: {} -> {} (load: {:.2}, mem_pressure: {:.2})",
                old,
                new_parallelism,
                load,
                mem_pressure
            );
        }

        new_parallelism
    }

    /// 根据任务特性选择执行策略
    pub fn select_strategy(&self, task: &Task) -> ExecutionStrategy {
        match task {
            Task::IncrementalUpdate { affected_shards } if *affected_shards < 10 => {
                // 小范围更新：串行避免调度开销
                ExecutionStrategy::Serial
            }
            Task::ColdBuild { .. } => {
                // 冷启动是长时间 I/O + metadata 扫描任务。并行 walker 会把 CPU
                // 长时间推到多核满载，压垮交互查询和事件处理；默认串行执行，把
                // materialize 成本留在后台，用更低 CPU 峰值换取稳定性。
                ExecutionStrategy::Serial
            }
            Task::VerifyGap { gap_size } if *gap_size > 1000 => {
                // 大缺口校验：弹性并行
                let adaptive_shards =
                    (*gap_size / 100).min(self.target_parallelism.load(Ordering::Relaxed) * 4);
                ExecutionStrategy::Parallel {
                    shards: adaptive_shards,
                    streaming: false,
                }
            }
            _ => ExecutionStrategy::Parallel {
                shards: self.target_parallelism.load(Ordering::Relaxed),
                streaming: false,
            },
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Task {
    IncrementalUpdate { affected_shards: usize },
    ColdBuild { total_dirs: usize },
    VerifyGap { gap_size: usize },
}

#[derive(Debug, Clone, Copy)]
pub enum ExecutionStrategy {
    Serial,
    Parallel { shards: usize, streaming: bool },
}
