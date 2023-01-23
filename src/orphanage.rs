use std::collections::HashMap;

use mongodb::bson;

use crate::{db, sharding};

// merge into super chunks
// for each super chunk, spawn a thread for each shard that is not this super chunks shard

// this range is on shard 02,

pub async fn find_orphaned(
    mongos: mongodb::Client,
    shards: HashMap<String, sharding::Shard>,
    ns: db::Namespace,
) -> mongodb::error::Result<()> {
    // let mut chunk_table = HashMap::new();
    let mut chunk_cursor = db::get_chunks_for_ns(mongos, ns).await?;
    while chunk_cursor.advance().await? {
        let chunk = chunk_cursor.deserialize_current()?;
        for (shard_key, shard) in &shards {
            if shard_key == &chunk.shard.as_str() {
                continue;
            }
            shard
                .tx
                .send(bson::doc! {"home_shard": &chunk.shard, "sending_to ": shard_key})
                .expect("fuck");
        }
    }
    Ok(())
}
