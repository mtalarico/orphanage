use cluster::ShardedCluster;
use tokio::sync::mpsc;

mod chunk;
mod cli;
mod cluster;
mod db;
mod shard;
mod util;

#[tokio::main]
async fn main() -> mongodb::error::Result<()> {
    let args = cli::args();
    let ns = mongodb::Namespace {
        db: args.db,
        coll: args.coll,
    };

    let (tx, mut rx) = mpsc::channel(128);
    let cluster = ShardedCluster::new(&args.uri).await?;

    tokio::spawn(async move { cluster.find_orphaned(ns.clone(), tx).await });
    let mut counter = 0;
    while let Some(orphan) = rx.recv().await {
        counter += 1;
        println!("got orphan {:?}", orphan)
    }
    println!("{counter}");
    Ok(())
}
