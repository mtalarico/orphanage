use std::collections::HashMap;

use mongodb::bson;

use crate::{chunk::Chunk, db, orphan::Orphans, shard::Shard, util, BUFFER_SIZE};

// pub struct ClusterClient(Standalone, ReplicaSet, Sharded);

pub struct Sharded {
    pub router: mongodb::Client,
    pub shards: HashMap<String, Shard>,
    pub shard_count: usize,
}

// pub struct Standalone {
//     pub client: mongodb::Client,
// }

// pub struct ReplicaSet {
//     pub client: mongodb::Client,
// }

impl Sharded {
    /// return a new sharded cluster, containing both a connection to the specified mongos pool and connection pools to each shard
    pub async fn new(uri: &str) -> mongodb::error::Result<Self> {
        let router = db::connect(uri).await?;
        let shard_connections = db::mongos::connect_to_shards(&router, uri).await?;
        let shard_count = shard_connections.len();

        let shards = HashMap::from_iter(shard_connections.iter().map(|(name, client)| {
            (
                name.to_owned(),
                Shard::new(name.as_str(), client.to_owned()),
            )
        }));

        Ok(Self {
            router,
            shards,
            shard_count,
        })
    }

    /// return orphans -- a struct that has summary data and a verbose map of orphans for each shard
    pub async fn find_orphaned(&self, ns: &mongodb::Namespace) -> mongodb::error::Result<Orphans> {
        log::info!("searching for orphans on namespace {}", &ns.to_string());

        let mut orphans = Orphans::new(self.shards.keys().collect::<Vec<&String>>());

        let shard_key = db::mongos::get_shard_key(&self.router, &ns).await?;

        // orphans.add(&shard.clone(), orphan.clone());
        // log::trace!("got orphan {:?}", &orphan);

        Ok(orphans)
    }

    /// push an optionally filtered stream of chunks over a channel
    async fn stream_chunks(
        &self,
        ns: Option<&mongodb::Namespace>,
        tx: tokio::sync::mpsc::Sender<Chunk>,
    ) {
        let mut chunk_cursor = Self::get_chunks_cursor(&self, ns).await.unwrap();
        while chunk_cursor.advance().await.unwrap() {
            let chunk = chunk_cursor.deserialize_current().unwrap();
            tx.send(chunk).await.unwrap();
        }
    }

    /// get a cursor iterating over the chunks collection, optionally filtering by namespace (supports both ns and uuid verions)
    async fn get_chunks_cursor(
        &self,
        ns: Option<&mongodb::Namespace>,
    ) -> mongodb::error::Result<mongodb::Cursor<Chunk>> {
        let mut filter: Option<bson::Document> = None;
        if let Some(ns) = ns {
            filter = Some(util::get_ns_filter(&self.router, &ns).await?);
        }
        let cursor = db::mongos::get_chunk_cursor(&self.router, filter).await?;
        Ok(cursor)
    }
}
