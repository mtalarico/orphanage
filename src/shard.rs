use futures::TryStreamExt;
use mongodb::bson;
use tokio::{sync::mpsc, task};

use crate::{chunk::Chunk, db::Id};

pub struct Shard {
    pub name: String,
    pub handle: task::JoinHandle<()>,
    pub job_tx: mpsc::Sender<Command>,
    pub result_rx: mpsc::Receiver<Response>,
}

impl Shard {
    pub async fn new(name: &str, client: mongodb::Client) -> mongodb::error::Result<Self> {
        let (job_tx, job_rx) = mpsc::channel::<Command>(1000);
        let (response_tx, result_rx) = mpsc::channel::<Response>(1000);

        let handle = tokio::spawn(async move {
            for job in job_rx {
                let response = Self::run_command(&client, job).await;
                let _ = response_tx.send(response);
            }
        });

        Ok(Self {
            name: String::from(name),
            handle,
            job_tx,
            result_rx,
        })
    }

    async fn run_command(client: &mongodb::Client, command: Command) -> Response {
        match command {
            Command::FindOrphanedId(shard_name, ns, chunk) => {
                let ids = find_orphaned_ids(shard_name, &client, &ns, chunk).await;
                Response::FindOrphanedId(ids)
            }
        }
    }
}

#[derive(Debug)]
pub enum Command {
    FindOrphanedId(String, mongodb::Namespace, Chunk),
}

#[derive(Debug)]

pub enum Response {
    FindOrphanedId(Vec<Id>),
}

async fn find_orphaned_ids(
    shard_name: String,
    client: &mongodb::Client,
    ns: &mongodb::Namespace,
    chunk: Chunk,
) -> Vec<Id> {
    let projection = bson::doc! { "_id": 1 };
    let options = mongodb::options::FindOptions::builder()
        .projection(projection)
        .build();
    // let filter = bson::doc! {};
    println!("searching for chunk {:?} on shard {:?}", chunk, shard_name);
    let result = client
        .database(ns.db.as_str())
        .collection::<Id>(ns.coll.as_str())
        .find(chunk.min, options)
        .await
        .expect("cannot execute find");
    result
        .try_collect()
        .await
        .expect("cannot iterate cursor to vec")
}
