use fd_rdd::index::{TieredIndex, L1Cache, L2Partition, L3Cold};
use fd_rdd::core::{FileIndexRDD, AdaptiveScheduler};
use fd_rdd::event::EventStream;
use fd_rdd::query::QueryServer;
use std::sync::Arc;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    
    info!("Starting fd-rdd: RDD-based elastic file indexer");
    
    // 初始化三级索引
    let mut roots = Vec::new();
    let test_data = std::path::PathBuf::from("/tmp/vcp_test_data");
    if test_data.exists() {
        roots.push(test_data);
    } else {
        let home_dir = dirs::home_dir().unwrap_or_else(|| std::path::PathBuf::from("/"));
        roots.push(home_dir);
    }
    
    let l1 = L1Cache::with_capacity(1000);
    let l2_rdd = FileIndexRDD::from_dirs(roots.clone());
    let l2 = L2Partition::new(l2_rdd);
    let l3 = L3Cold::adaptive();
    
    let index = Arc::new(TieredIndex::new(l1, l2, l3, roots.clone()));
    
    // 启动事件流
    let event_stream = EventStream::new(index.clone());
    event_stream.start_watcher(roots).await?;
    
    // 启动查询服务 (HTTP)
    let query_server = QueryServer::new(index.clone());
    tokio::spawn(query_server.run(6060));
    
    // 启动自适应调度器 (模拟运行)
    let mut adaptive_scheduler = AdaptiveScheduler::new();
    tokio::spawn(async move {
        loop {
            adaptive_scheduler.adjust_parallelism();
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        }
    });
    
    info!("fd-rdd ready. Query via: http://localhost:6060/search?q=keyword");
    
    // 优雅退出处理
    tokio::signal::ctrl_c().await?;
    info!("Shutting down...");
    
    Ok(())
}
