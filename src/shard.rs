use mongodb::bson;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc};

use crate::chunk::Chunk;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Orphan {
    #[serde(skip_serializing, skip_deserializing)]
    pub shard: String,
    pub _id: bson::oid::ObjectId,
}

#[derive(Debug, Clone)]
pub struct Shard {
    pub name: String,
    client: mongodb::Client,
}

#[derive(Debug, Clone)]
pub enum Command {
    FindOrphanedId {
        ns: mongodb::Namespace,
        chunk: Chunk,
        rsp_chnnl: mpsc::Sender<Orphan>,
    },
}

impl Shard {
    pub async fn new(
        name: &str,
        client: mongodb::Client,
        mut rx: broadcast::Receiver<Command>,
    ) -> mongodb::error::Result<Self> {
        let me = Self {
            name: String::from(name),
            client: client.clone(),
        };
        tokio::spawn(async move {
            while let Ok(command) = rx.recv().await {
                me.run_command(command).await;
            }
        });

        Ok(Self {
            name: String::from(name),
            client: client.clone(),
        })
    }

    async fn run_command(&self, command: Command) {
        match command {
            Command::FindOrphanedId {
                ns,
                chunk,
                rsp_chnnl,
            } => {
                // dont search for orphans on the source of truth shard
                if self.name == chunk.shard {
                    return;
                }
                let mut orphans = self.find_orphaned_ids(&ns, chunk).await;
                while orphans.advance().await.expect("cannot advance id cursor") {
                    let mut orphan = orphans.deserialize_current().expect("cannot parse id");
                    orphan.shard = self.name.clone();
                    let _ = rsp_chnnl.send(orphan).await;
                }
            }
        }
    }

    async fn find_orphaned_ids(
        &self,
        ns: &mongodb::Namespace,
        chunk: Chunk,
    ) -> mongodb::Cursor<Orphan> {
        let projection = bson::doc! { "_id": 1 };
        let options = mongodb::options::FindOptions::builder()
            .projection(projection)
            .build();
        println!("searching for chunk {:?} on shard {:?}", chunk, &self.name);
        let filter = chunk.filter_for_range();
        let result = self
            .client
            .database(ns.db.as_str())
            .collection::<Orphan>(ns.coll.as_str())
            .find(filter, options)
            .await
            .expect("cannot execute find");
        result
    }
}
