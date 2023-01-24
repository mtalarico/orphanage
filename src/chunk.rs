use mongodb::bson;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Chunk {
    pub shard: String,
    pub min: bson::Document,
    pub max: bson::Document,
}

pub fn merge_or_add(chunk: &Chunk, chunks: &mut Vec<Chunk>) {
    let mut combined = false;
    for each in chunks.iter_mut() {
        // Let this chunk = {A_min, A_max} and the chunk we are comparing = { B_min, B_max }
        if chunk.shard == each.shard && chunk.max == each.min {
            // if A_max = B_min, make the new megachunk = { A_min, B_max }
            each.min = chunk.min.clone();
            combined = true;
        } else if chunk.shard == each.shard && chunk.min == each.max {
            // if A_min = B_max, make the new megachunk = { B_min, A_max }
            each.max = chunk.max.clone();
            combined = true;
        }
    }
    if combined == false {
        chunks.push(chunk.clone());
    }
}
