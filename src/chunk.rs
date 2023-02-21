use mongodb::bson;
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct Chunk {
    pub shard: String,
    pub min: bson::Document,
    pub max: bson::Document,
}
