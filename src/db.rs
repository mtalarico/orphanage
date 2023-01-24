use std::collections::HashMap;

use futures::TryStreamExt;
use mongodb::bson;
use serde::{Deserialize, Serialize};

use crate::{chunk, util};

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct Id {
    pub _id: bson::oid::ObjectId,
}

/// connects to instance at uri, specify options and credentials according to mongodb docs
pub async fn connect(uri: &str) -> mongodb::error::Result<mongodb::Client> {
    let mut options = mongodb::options::ClientOptions::parse(uri).await?;
    options.app_name = Some("orphanage".to_string());
    let client = mongodb::Client::with_options(options)?;
    client
        .database("admin")
        .run_command(bson::doc! {"ping": 1}, None)
        .await?;
    println!("Connected to {}", uri);
    Ok(client)
}

/// connect to all shards of a given mongos client, returning a hashmap of shard names to clients
pub async fn connect_to_shards(
    mongos: &mongodb::Client,
) -> mongodb::error::Result<HashMap<String, mongodb::Client>> {
    let mut shard_map = HashMap::new();
    let mut shards = mongos
        .database("config")
        .collection::<bson::Document>("shards")
        .find(None, None)
        .await?;
    while let Some(doc) = shards.try_next().await? {
        let original_uri = doc
            .get_str("host")
            .expect("cannot parse shard connection string from config.shards");
        let uri = util::update_connection_string(original_uri);
        let key = doc
            .get_str("_id")
            .expect("cannot parse shard key from config.shards");
        let client = connect(uri.as_str());

        shard_map.insert(String::from(key), client.await?);
    }
    Ok(shard_map)
}

pub async fn get_chunk_cursor(
    mongos: &mongodb::Client,
    filter: Option<bson::Document>,
) -> mongodb::error::Result<mongodb::Cursor<chunk::Chunk>> {
    let cursor = mongos
        .database("config")
        .collection::<chunk::Chunk>("chunks")
        .find(filter, None)
        .await?;
    Ok(cursor)
}

/// returns true if connected client is pointed at mongos process, false or error
pub async fn is_mongos(client: &mongodb::Client) -> bool {
    let res = client
        .database("admin")
        .run_command(bson::doc! {"isdbgrid": 1}, None)
        .await;
    if res.is_err() {
        if !util::isdbgrid_error(res.err().unwrap()) {
            panic!("an error occured checking whether client is mongos");
        }
        return false;
    }
    true
}

pub async fn get_version(client: &mongodb::Client) -> mongodb::error::Result<String> {
    let doc = client
        .database("admin")
        .run_command(bson::doc! {"serverStatus": 1}, None)
        .await?;
    let version = doc.get_str("version").expect("cannot get server version");
    Ok(String::from(version))
}
