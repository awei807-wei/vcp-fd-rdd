use std::sync::atomic::{AtomicUsize, Ordering};
use sysinfo::System;

/// 自适应调度器（类 Spark Adaptive Execution）
pub struct AdaptiveScheduler {
    target_parallelism: AtomicUsize,
    system: System,
}

impl AdaptiveScheduler {
    pub fn new() -> Self {
        let mut sys = System::new_all();
        sys.refresh_all();
        
        Self {
            target_parallelism: AtomicUsize::new(num_cpus::get()),
            system: sys,
        }
    }
    
    /// 动态调整并行度
    pub fn adjust_parallelism(&mut self) -> usize {
        self.system.refresh_all();
        
        let load = System::load_average().one;
        let cpu_count = num_cpus::get() as f64;
        let mem_free = self.system.available_memory();
        let total_mem = self.system.total_memory();
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
        
        let old = self.target_parallelism.swap(new_parallelism, Ordering::Relaxed);
        
        if old != new_parallelism {
            tracing::info!(
                "Adaptive parallelism: {} -> {} (load: {:.2}, mem_pressure: {:.2})",
                old, new_parallelism, load, mem_pressure
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
            Task::ColdBuild { total_dirs } if *total_dirs > 10000 => {
                // 大规模冷启动：分片并行 + 流式
                ExecutionStrategy::Parallel {
                    shards: self.target_parallelism.load(Ordering::Relaxed) * 2,
                    streaming: true,
                }
            }
            Task::VerifyGap { gap_size } if *gap_size > 1000 => {
                // 大缺口校验：弹性并行
                let adaptive_shards = (*gap_size / 100).min(
                    self.target_parallelism.load(Ordering::Relaxed) * 4
                );
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

pub enum Task {
    IncrementalUpdate { affected_shards: usize },
    ColdBuild { total_dirs: usize },
    VerifyGap { gap_size: usize },
}

pub enum ExecutionStrategy {
    Serial,
    Parallel { shards: usize, streaming: bool },
}