use std::collections::HashMap;

use mongodb::bson;
use tokio::sync::{broadcast, mpsc};

use crate::{
    chunk::{self, Chunk},
    db::{self, Id},
    shard::{Command, Shard},
    util,
};

pub struct ShardedCluster {
    pub router: mongodb::Client,
    pub shards: HashMap<String, Shard>,
    _broadcast_tx: broadcast::Sender<Command>,
    _broadcast_rx: broadcast::Receiver<Command>,
}

impl ShardedCluster {
    /// return a new sharded cluster, containing both a connection to a mongos pool and a connection to all shards
    pub async fn new(uri: &str) -> mongodb::error::Result<Self> {
        let router = db::connect(uri).await?;
        let shard_connections = db::connect_to_shards(&router);

        let (tx, rx) = broadcast::channel::<Command>(128);
        let mut shards: HashMap<String, Shard> = HashMap::new();
        assert_mongos(&router).await?;
        for (shard_name, shard_client) in shard_connections.await? {
            let shard = Shard::new(&shard_name, shard_client.clone(), tx.subscribe()).await?;
            shards.insert(shard_name, shard);
        }

        Ok(Self {
            router: router.clone(),
            shards: shards,
            _broadcast_tx: tx,
            _broadcast_rx: rx,
        })
    }

    /// get list of chunks (optimized)
    pub async fn get_megachunks(
        &self,
        ns: Option<&mongodb::Namespace>,
    ) -> mongodb::error::Result<Vec<Chunk>> {
        let mut chunk_cursor = Self::get_chunks_cursor(&self, ns).await?;
        let mut megachunks: Vec<Chunk> = Vec::new();
        while chunk_cursor.advance().await? {
            let chunk = chunk_cursor.deserialize_current()?;
            chunk::merge_or_add(&chunk, &mut megachunks)
        }
        Ok(megachunks)
    }

    ///return a channel that streams orphan docs from shards
    pub async fn find_orphaned(
        &self,
        ns: mongodb::Namespace,
        tx: mpsc::Sender<Id>,
    ) -> mongodb::error::Result<()> {
        let chunks = self.get_megachunks(Some(&ns)).await?;
        for chunk in chunks {
            let command = Command::FindOrphanedId {
                ns: ns.clone(),
                chunk: chunk.clone(),
                rsp_chnnl: tx.clone(),
            };
            let _ = self._broadcast_tx.send(command);
        }
        Ok(())
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
        let cursor = db::get_chunk_cursor(&self.router, filter);
        Ok(cursor.await?)
    }
}

/// exits the program with an error if client is not pointing at mongos process
async fn assert_mongos(client: &mongodb::Client) -> mongodb::error::Result<()> {
    if !db::is_mongos(client).await {
        eprintln!("Error: Must connect to mongos");
        std::process::exit(1);
    }
    Ok(())
}
