use std::{collections::HashMap, sync::Arc};

use futures::{future::join_all, StreamExt};

use crate::{
    db::{self, Id},
    orphan::Orphans,
    util, BUFFER_SIZE,
};

// pub struct ClusterClient(Standalone, ReplicaSet, Sharded);

#[derive(Debug)]
pub struct ShardedCluster {
    pub router: mongodb::Client,
    pub shards: HashMap<String, mongodb::Client>,
}

// pub struct Standalone {
//     pub client: mongodb::Client,
// }

// pub struct ReplicaSet {
//     pub client: mongodb::Client,
// }

impl ShardedCluster {
    /// return a new sharded cluster, containing both a connection to the specified mongos pool and connection pools to each shard
    pub async fn new(uri: &str) -> mongodb::error::Result<Self> {
        let router = db::connect(uri).await?;
        let shards = db::mongos::connect_to_shards(&router, uri).await?;

        Ok(Self { router, shards })
    }

    /// get the number of shards currently connected to
    // pub fn get_shard_count(&self) -> usize {
    //     self.shards.len()
    // }

    /// return orphans -- a struct that has summary data and a verbose map of orphans for each shard
    pub async fn find_orphaned(&self, ns: &mongodb::Namespace) -> mongodb::error::Result<Orphans> {
        log::info!("searching for orphans on namespace {}", &ns.to_string());
        let ns = Arc::new(ns.to_owned());

        let router_ref = self.router.clone();
        let ns_ref = ns.clone();
        let shard_key_task = tokio::spawn(async move {
            db::mongos::get_shard_key(&router_ref, &ns_ref)
                .await
                .unwrap()
        });

        let router_ref = self.router.clone();
        let ns_ref = ns.clone();
        let chunks_task = tokio::spawn(async move {
            let filter = Some(util::get_ns_filter(&router_ref, &ns_ref).await.unwrap());
            db::mongos::get_chunk_cursor(&router_ref, filter)
                .await
                .unwrap()
        });

        let mut orphans = Orphans::new(self.shards.keys().collect::<Vec<&String>>());
        let ns = Arc::new(ns.clone());

        let (shard_key_res, chunks_cursor_res) = tokio::join!(shard_key_task, chunks_task);
        let shard_key = Arc::new(shard_key_res.unwrap());
        log::info!("shard key for ns {} is {}", &ns, &shard_key.to_string());
        let mut chunks_cursor = chunks_cursor_res.unwrap();

        let (tx, mut rx) = tokio::sync::mpsc::channel::<(String, Id)>(BUFFER_SIZE);
        let orphans = tokio::spawn(async move {
            while let Some((shard, orphan_id)) = rx.recv().await {
                log::debug!("adding {:?} to shard {}", &orphan_id, &shard);
                orphans.add(&shard, orphan_id);
            }
            orphans
        });

        let mut tasks = Vec::new();
        while let Some(chunk) = chunks_cursor.next().await.transpose().unwrap() {
            let shards = self.shards.clone();
            let chunk = Arc::new(chunk);

            let shards_minus_self = shards
                .into_iter()
                .filter(|(shard_name, _)| shard_name.to_owned() != chunk.clone().shard);
            for (shard_name, client) in shards_minus_self {
                let ns = ns.clone();
                let shard_key = shard_key.clone();
                let chunk = chunk.clone();
                let tx = tx.clone();
                let handle = tokio::spawn(async move {
                    let mut chunk_ids =
                        db::find_id_range(&client, &ns, &shard_key.clone(), &chunk.min, &chunk.max)
                            .await;
                    while let Some(id) = chunk_ids.next().await {
                        log::debug!("found {:?} on shard {}", &id.clone().unwrap(), &shard_name);
                        tx.send((shard_name.clone(), id.unwrap())).await.unwrap();
                    }
                    drop(tx);
                });
                tasks.push(handle);
            }
        }
        join_all(tasks).await;
        // drop the original tx that was cloned and is no longer needed
        drop(tx);
        let orphans = orphans.await.unwrap();

        Ok(orphans)
    }
}
