use std::collections::HashMap;

use cluster::ShardedCluster;
use mongodb::bson;
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
    let mut orphan_map: HashMap<String, Vec<bson::oid::ObjectId>> =
        HashMap::with_capacity(cluster.shard_count);
    for shard in cluster.shards.keys() {
        orphan_map.insert(shard.clone(), Vec::<bson::oid::ObjectId>::new());
    }

    tokio::spawn(async move { cluster.find_orphaned(ns.clone(), tx).await });
    let mut counter = 0;
    while let Some(orphan) = rx.recv().await {
        counter += 1;
        let shard = orphan.shard.clone();
        orphan_map.get_mut(&shard).unwrap().push(orphan._id);
        // println!("got orphan {:?}", orphan)
    }
    // println!("{:?}", orphan_map);
    Ok(())
}
