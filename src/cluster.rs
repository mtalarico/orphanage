use std::collections::HashMap;

use crate::db;

#[derive(Debug)]
pub struct ShardedCluster {
    pub router: mongodb::Client,
    // pub shards: HashMap<String, mongodb::Client>,
}

impl ShardedCluster {
    /// return a struct containing both a connection to the specified routers and connections to each shard
    pub async fn new(uri: &str) -> mongodb::error::Result<Self> {
        let router = db::connect(uri).await?;
        // let shards = db::mongos::connect_to_shards(&router, uri).await?;

        // Ok(Self { router, shards })
        Ok(Self { router })
    }

    pub async fn count_orphaned(
        &self,
        ns: &mongodb::Namespace,
    ) -> mongodb::error::Result<HashMap<String, i32>> {
        log::info!("counting orphans");
        let shard_key = db::mongos::get_shard_key(&self.router, &ns).await?;
        let explain_plan = db::covered_explain(&self.router, &ns, None, shard_key).await?;
        // log::info!("{:#?}", explain_plan);
        let shards = explain_plan
            .get_document("executionStats")
            .unwrap()
            .get_document("executionStages")
            .unwrap()
            .get_array("shards")
            .unwrap();

        let mut results: HashMap<String, i32> = HashMap::new();
        for shard in shards {
            let shard = shard.as_document().unwrap();
            let orphan_count = shard
                .get_document("executionStages")
                .unwrap()
                .get_document("inputStage")
                .unwrap()
                .get_i32("chunkSkips")
                .unwrap();
            let shard_name = shard.get_str("shardName").unwrap();
            results.insert(shard_name.to_string(), orphan_count);
        }

        return Ok(results);
    }
}
