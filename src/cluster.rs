use std::collections::HashMap;

use mongodb::bson;

use crate::{
    chunk, db,
    shard::{self, Shard},
    util,
};

pub struct ShardedCluster {
    pub router: mongodb::Client,
    pub shards: HashMap<String, Shard>,
}

impl ShardedCluster {
    /// return a new sharded cluster, containing both a connection to a mongos pool and a connection to all shards
    pub async fn new(uri: &str) -> mongodb::error::Result<Self> {
        let router = db::connect(uri).await?;
        let shard_connections = db::connect_to_shards(&router);
        let mut shards: HashMap<String, Shard> = HashMap::new();
        assert_mongos(&router).await?;
        for (shard_name, shard_client) in shard_connections.await? {
            let shard = Shard::new(&shard_name, shard_client).await?;
            shards.insert(shard_name, shard);
        }

        Ok(Self {
            router: router.clone(),
            shards: shards,
        })
    }

    /// get list of chunks (optimized)
    pub async fn get_megachunks(
        &self,
        ns: Option<&mongodb::Namespace>,
    ) -> mongodb::error::Result<Vec<chunk::Chunk>> {
        let mut chunk_cursor = Self::get_chunks_cursor(&self, ns).await?;
        let mut megachunks: Vec<chunk::Chunk> = Vec::new();
        while chunk_cursor.advance().await? {
            let chunk = chunk_cursor.deserialize_current()?;
            chunk::merge_or_add(&chunk, &mut megachunks)
        }
        Ok(megachunks)
    }

    ///return a channel that streams orphan docs from shards
    pub async fn find_orphaned(&self, ns: &mongodb::Namespace) -> mongodb::error::Result<()> {
        let chunks = self.get_megachunks(Some(&ns)).await?;
        for chunk in chunks {
            for (shard_key, shard) in &self.shards {
                if shard_key == &chunk.shard {
                    continue;
                }
                let command =
                    shard::Command::FindOrphanedId(shard_key.clone(), ns.clone(), chunk.clone());
                shard
                    .job_tx
                    .send(command)
                    .await
                    .expect("error sending command to thread");
            }
        }
        Ok(())
    }

    /// get a cursor iterating over the chunks collection, optionally filtering by namespace (supports both ns and uuid verions)
    async fn get_chunks_cursor(
        &self,
        ns: Option<&mongodb::Namespace>,
    ) -> mongodb::error::Result<mongodb::Cursor<chunk::Chunk>> {
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
