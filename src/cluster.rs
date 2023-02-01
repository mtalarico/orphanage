use std::{collections::HashMap, sync::Arc};

use futures::{future::join_all, TryStreamExt};

use crate::{
    db::{self},
    orphan::{Orphan, OrphanSummary},
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
    /// return a struct containing both a connection to the specified routers and connections to each shard
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
    pub async fn find_orphaned(
        &self,
        ns: &mongodb::Namespace,
    ) -> mongodb::error::Result<OrphanSummary> {
        log::info!("searching for orphans on namespace {}", &ns.to_string());
        let ns = Arc::new(ns.to_owned());

        // get the shard key in a background task
        let router_ref = self.router.clone();
        let ns_ref = ns.clone();
        let shard_key_task = tokio::spawn(async move {
            db::mongos::get_shard_key(&router_ref, &ns_ref)
                .await
                .unwrap()
        });

        // get the chunk cursor in a background task
        let router_ref = self.router.clone();
        let ns_ref = ns.clone();
        let chunks_task = tokio::spawn(async move {
            let filter = Some(util::get_ns_filter(&router_ref, &ns_ref).await.unwrap());
            db::mongos::get_chunk_cursor(&router_ref, filter)
                .await
                .unwrap()
        });

        // join threads back together
        let (shard_key_res, chunks_cursor_res) = tokio::join!(shard_key_task, chunks_task);
        let shard_key = Arc::new(shard_key_res.unwrap());
        let mut chunks_cursor = chunks_cursor_res.unwrap();
        log::info!("shard key for ns {} is {}", &ns, &shard_key.to_string());

        // create a multi-producer single consumer channel and listen for orphans, adding them to the summary as they are processed
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Orphan>(BUFFER_SIZE);
        let mut summary = OrphanSummary::new(self.shards.keys().collect::<Vec<&String>>());
        let handle = tokio::spawn(async move {
            while let Some(orphan) = rx.recv().await {
                log::debug!("adding orphan {:?}", &orphan);
                summary.add(orphan);
            }
            summary
        });

        // iterate through chunks cursor, spawning background threads to send each chunk to every shard except its own, if any results are found put them on the orphan channel
        let mut tasks = Vec::new();
        while let Some(chunk) = chunks_cursor.try_next().await? {
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
                    while let Some(id) = chunk_ids.try_next().await.unwrap() {
                        log::debug!("found {:?} on shard {}", &id, &shard_name);
                        let orphan = Orphan {
                            shard: shard_name.clone(),
                            id: id,
                        };
                        tx.send(orphan).await.unwrap();
                    }
                    drop(tx);
                });
                tasks.push(handle);
            }
        }

        // ensure all tasks have finished, drop the original tx and wait for the reciever to process it all before returning the completed summary
        join_all(tasks).await;
        drop(tx);
        let summary = handle.await.unwrap();

        Ok(summary)
    }
}
