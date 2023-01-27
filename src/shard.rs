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
                let colls = self
                    .client
                    .database(&ns.db)
                    .list_collection_names(None)
                    .await
                    .expect("error finding ns");
                if !&colls.contains(&ns.coll) {
                    log::debug!(
                        "ns {:?} not found on shard {:?} skipping",
                        &ns.to_string(),
                        &self.name
                    );
                }
                let mut orphans = self.find_orphaned_ids(&ns, chunk).await;
                while orphans
                    .advance()
                    .await
                    .expect("cannot advance orphan cursor")
                {
                    let doc = orphans
                        .deserialize_current()
                        .expect("cannot read doc from cursor");
                    let mut orphan: Orphan =
                        bson::from_document(doc).expect("cannot deseralize orphan");
                    orphan.shard = self.name.clone();
                    log::trace!("{:?}", &orphan);
                    let _ = rsp_chnnl.send(orphan).await;
                }
            }
        }
    }

    async fn find_orphaned_ids(
        &self,
        ns: &mongodb::Namespace,
        chunk: Chunk,
    ) -> mongodb::Cursor<bson::Document> {
        let projection = bson::doc! { "$project": { "_id": 1 }};
        let filter = bson::doc! { "$match": chunk.filter_for_range()};
        log::debug!(
            "searching for chunk {:?} on shard {:?} with filter {:?}",
            chunk,
            &self.name,
            &filter
        );
        let result = self
            .client
            .database(ns.db.as_str())
            .collection::<Orphan>(ns.coll.as_str())
            .aggregate([filter, projection], None)
            .await
            .expect("cannot execute find");
        result
    }
}
