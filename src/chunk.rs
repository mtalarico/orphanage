use mongodb::bson;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Chunk {
    pub shard: String,
    pub min: bson::Document,
    pub max: bson::Document,
}
