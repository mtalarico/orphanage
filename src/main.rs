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
    while let Some(id) = rx.recv().await {
        println!("got id {:?}", id)
    }
    Ok(())
}
