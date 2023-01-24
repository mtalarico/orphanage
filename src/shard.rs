use mongodb::bson;
use tokio::sync::{broadcast, mpsc};

use crate::{chunk::Chunk, db::Id};

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
        rsp_chnnl: mpsc::Sender<Id>,
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
                let mut ids = self.find_orphaned_ids(&ns, chunk).await;
                while ids.advance().await.expect("cannot advance id cursor") {
                    let id = ids.deserialize_current().expect("cannot parse id");
                    let _ = rsp_chnnl.send(id).await;
                }
            }
        }
    }

    async fn find_orphaned_ids(
        &self,
        ns: &mongodb::Namespace,
        chunk: Chunk,
    ) -> mongodb::Cursor<Id> {
        let projection = bson::doc! { "_id": 1 };
        let options = mongodb::options::FindOptions::builder()
            .projection(projection)
            .build();
        println!("searching for chunk {:?} on shard {:?}", chunk, &self.name);
        let result = self
            .client
            .database(ns.db.as_str())
            .collection::<Id>(ns.coll.as_str())
            .find(chunk.min, options)
            .await
            .expect("cannot execute find");
        result
    }
}
