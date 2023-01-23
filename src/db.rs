use std::collections::HashMap;

use futures::TryStreamExt;
use mongodb::bson;

use crate::{sharding, util};

pub struct Id {
    pub _id: bson::oid::ObjectId,
}

#[derive(Debug, Copy, Clone)]
pub struct Namespace {
    pub db: &'static str,
    pub coll: &'static str,
}

impl Namespace {
    pub fn as_str(self) -> &'static str {
        &[self.db, self.coll].join(".")
    }
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

/// connect to all shards of a given mongos client, returning a hashmap of shard names to channels
pub async fn connect_to_shards(
    mongos: &mongodb::Client,
) -> mongodb::error::Result<HashMap<String, sharding::Shard>> {
    let mut shard_map = HashMap::new();
    let mut shards = mongos
        .database("config")
        .collection::<bson::Document>("shards")
        .find(None, None)
        .await?;
    while let Some(doc) = shards.try_next().await? {
        let shard = sharding::Shard::new(&doc, &ns).await?;
        let key = doc
            .get_str("_id")
            .expect("cannot parse shard key from config.shards");
        shard_map.insert(String::from(key), shard);
    }
    Ok(shard_map)
}

/// exits the program with an error if client is not pointing at mongos process
pub async fn assert_mongos(client: &mongodb::Client) -> mongodb::error::Result<()> {
    if !is_mongos(&client).await {
        eprintln!("Error: Must connect to mongos");
        std::process::exit(1);
    }
    Ok(())
}

pub async fn get_chunks_for_ns(
    client: mongodb::Client,
    ns: Namespace,
) -> mongodb::error::Result<mongodb::Cursor<sharding::Chunk>> {
    let filter: bson::Document;
    let version = util::get_cluster_version(&client).await?;

    if util::version_above_5(version.as_str()) == true {
        let uuid = util::get_uuid_for_ns(&client, &ns).await?;
        filter = bson::doc! { "uuid": uuid };
    } else {
        filter = bson::doc! {"ns": ns.as_str()};
    }
    let cursor = client
        .database("config")
        .collection::<sharding::Chunk>("chunks")
        .find(filter, None)
        .await?;
    Ok(cursor)
}

/// returns true if connected client is pointed at mongos process, false or error
async fn is_mongos(client: &mongodb::Client) -> bool {
    let res = client
        .database("admin")
        .run_command(bson::doc! {"isdbgrid": 1}, None)
        .await;
    if res.is_err() {
        if !util::isdbgrid_error(res.err().unwrap()) {
            panic!("fuck an error");
        }
        return false;
    }
    true
}
