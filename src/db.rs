use mongodb::bson;

use crate::util;

/// connects to instance at uri, specify options and credentials according to mongodb docs (https://www.mongodb.com/docs/manual/reference/connection-string/)
pub async fn connect(uri: &str) -> mongodb::error::Result<mongodb::Client> {
    let mut options = mongodb::options::ClientOptions::parse(uri).await?;
    options.app_name = Some("orphanage".to_string());
    let client = mongodb::Client::with_options(options)?;
    client
        .database("admin")
        .run_command(bson::doc! {"ping": 1}, None)
        .await?;
    log::debug!("Connected to {}", uri);
    Ok(client)
}

pub async fn covered_explain(
    client: &mongodb::Client,
    ns: &mongodb::Namespace,
    filter: Option<bson::Document>,
    index: bson::Document,
) -> mongodb::error::Result<bson::Document> {
    let mut proj = util::make_projection(&index);
    proj.insert("_id", 0);

    let command = bson::doc! {
        "explain": {
            "find": &ns.coll,
            "filter": filter.unwrap_or_default(),
            "projection": proj,
            "hint": index
        }
    };
    log::debug!("running command {:?}", command);
    client.database(&ns.db).run_command(command, None).await
}

pub mod mongos {
    // use std::collections::HashMap;

    // use futures::{future, TryStreamExt};
    use mongodb::bson;
    use serde::{Deserialize, Serialize};

    use crate::util;

    // const SHARD_STEADY_STATE: usize = 1;

    #[derive(Debug, Clone, Deserialize, Serialize)]
    struct ShardDoc {
        _id: String,
        host: String,
        state: usize,
    }

    /// connect to all shards of a given mongos client, returning a hashmap of shard names to clients
    ///
    /// attempts to connect to shards in parallel
    // pub async fn connect_to_shards(
    //     mongos: &mongodb::Client,
    //     uri: &str,
    // ) -> mongodb::error::Result<HashMap<String, mongodb::Client>> {
    //     assert_mongos(&mongos).await?;

    //     let mut shard_names = Vec::new();
    //     let mut tasks = Vec::new();

    //     let mut shards_cursor = mongos
    //         .database("config")
    //         .collection::<ShardDoc>("shards")
    //         .find(None, None)
    //         .await?;
    //     while let Some(shard) = shards_cursor.try_next().await? {
    //         if shard.state != SHARD_STEADY_STATE {
    //             log::warn!("skipping shard {}, is not in a steady state", shard._id);
    //             continue;
    //         }
    //         let updated_uri = util::update_connection_string(uri, shard.host.as_str());

    //         // maintaining two separate ordered lists so clients will be connected to in parallel tasks
    //         shard_names.push(shard._id);
    //         tasks.push(tokio::spawn(async move {
    //             log::debug!("Connecting to {}", &updated_uri);
    //             super::connect(updated_uri.as_str()).await.unwrap()
    //         }));
    //     }

    //     // wait for all tasks to finish, panics if any connection issue to any of the shards
    //     // note that join_all is guarenteed to return in the provided order so no concern zipping arrays back up
    //     let connected_client = future::join_all(tasks)
    //         .await
    //         .into_iter()
    //         .map(|x| x.unwrap())
    //         .collect::<Vec<mongodb::Client>>();

    //     let zipped = shard_names.into_iter().zip(connected_client.into_iter());
    //     let shard_map = HashMap::from_iter(zipped);
    //     Ok(shard_map)
    // }

    /// get a document for the shard key used to shard a collection
    pub async fn get_shard_key(
        mongos: &mongodb::Client,
        ns: &mongodb::Namespace,
    ) -> mongodb::error::Result<bson::Document> {
        assert_mongos(&mongos).await?;

        let filter = bson::doc! { "_id": ns.to_string() };
        let doc = mongos
            .database("config")
            .collection::<bson::Document>("collections")
            .find_one(filter, None)
            .await?
            .expect("Unable to find sharded ns");
        let key = doc.get("key").unwrap().as_document().unwrap().to_owned();
        log::debug!("got shard key {:?}", key);
        Ok(key)
    }

    /// returns true if connected client is pointed at mongos process, false if not. panics if error is not related to isdbgrid
    /// useful for splitting code path based on client type
    pub async fn is_mongos(client: &mongodb::Client) -> bool {
        let res = client
            .database("admin")
            .run_command(bson::doc! {"isdbgrid": 1}, None)
            .await;
        if let Err(err) = res {
            if !util::isdbgrid_error(err) {
                panic!("an unrelated error occured checking whether client is mongos");
            }
            return false;
        }
        true
    }

    /// Wrapper function around is_mongos that exits the program with an error if client is not pointing at one or more mongos process
    pub async fn assert_mongos(client: &mongodb::Client) -> mongodb::error::Result<()> {
        if !is_mongos(client).await {
            log::error!("Error: Must connect to mongos");
            std::process::exit(1);
        }
        Ok(())
    }
}
