use std::sync::mpsc::{self, Sender};
use std::thread::{self, JoinHandle};

use futures::channel::oneshot::Receiver;
use mongodb::bson;
use serde::{Deserialize, Serialize};

use crate::{db, util};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Chunk {
    pub shard: String,
    pub min: bson::Document,
    pub max: bson::Document,
}

pub struct Shard {
    pub handle: JoinHandle<()>,
    pub tx: Sender<Vec<db::Id>>,
    rx: Receiver<fn()>,
}

impl ShardRunner {
    /// create a new Shard from a config.shard doc,
    pub async fn new(
        shard_doc: &bson::Document,
        ns: &'static db::Namespace,
    ) -> mongodb::error::Result<Self> {
        let original_uri = shard_doc
            .get_str("host")
            .expect("cannot parse shard connection string from config.shards");
        let uri = util::update_connection_string(original_uri);
        let client = db::connect(uri.as_str()).await?;

        let (tx, rx) = mpsc::channel::<fn()>();

        let handle = thread::spawn(move || {
            for command in rx {
                let reSelf::run_command(&client, command);
            }
        });

        let shard = Self {
            handle: handle,
            tx: tx,
            rx:
        };
        Ok(shard)
    }

    async fn run_command(&client: mongodb::Client, command: fn()) {

    }
}

pub async fn find_docs(
    shard: &mongodb::Client,
    ns: &db::Namespace,
    filter: bson::Document,
) -> mongodb::error::Result<bson::Document> {
    // bson::doc! { };
    let res = shard
        .database(ns.db)
        .collection::<db::Id>(ns.coll)
        .find(filter, None)
        .await?;
}
