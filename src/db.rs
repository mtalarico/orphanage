use mongodb::bson;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Id {
    _id: bson::oid::ObjectId,
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
    log::debug!("Connected to {}", uri);
    Ok(client)
}

pub async fn get_version(client: &mongodb::Client) -> mongodb::error::Result<String> {
    let doc = client
        .database("admin")
        .run_command(bson::doc! {"serverStatus": 1}, None)
        .await?;
    let version = doc.get_str("version").expect("cannot get server version");
    Ok(String::from(version))
}

pub mod mongos {
    use std::collections::HashMap;

    use mongodb::bson;
    use serde::{Deserialize, Serialize};

    use crate::{chunk::Chunk, util};

    use super::{connect, Id};

    #[derive(Debug, Clone, Deserialize, Serialize)]
    struct ShardDoc {
        _id: String,
        host: String,
        state: usize,
    }

    /// connect to all shards of a given mongos client, returning a hashmap of shard names to clients
    pub async fn connect_to_shards(
        mongos: &mongodb::Client,
        uri: &str,
    ) -> mongodb::error::Result<HashMap<String, mongodb::Client>> {
        assert_mongos(&mongos).await?;

        let mut shard_map = HashMap::new();
        let mut shards_cursor = mongos
            .database("config")
            .collection::<ShardDoc>("shards")
            .find(None, None)
            .await?;
        while shards_cursor.advance().await? {
            let shard = shards_cursor.deserialize_current()?;
            let uri = util::update_connection_string(uri, shard.host.as_str());
            let client = connect(uri.as_str());

            shard_map.insert(shard._id, client.await?);
        }
        Ok(shard_map)
    }

    pub async fn get_chunk_cursor(
        mongos: &mongodb::Client,
        filter: Option<bson::Document>,
    ) -> mongodb::error::Result<mongodb::Cursor<Chunk>> {
        assert_mongos(&mongos)
            .await
            .expect("problem checking mongos");
        let cursor = mongos
            .database("config")
            .collection::<Chunk>("chunks")
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

    pub async fn get_shard_key(
        mongos: &mongodb::Client,
        ns: &mongodb::Namespace,
    ) -> mongodb::error::Result<bson::Document> {
        assert_mongos(&mongos)
            .await
            .expect("problem checking mongos");
        let filter = bson::doc! { "_id": ns.to_string() };
        log::debug!("{}", &filter);
        let doc = mongos
            .database("config")
            .collection::<bson::Document>("collections")
            .find_one(filter, None)
            .await?;
        Ok(doc
            .expect("ns does not exist in config.collections")
            .get("key")
            .unwrap()
            .as_document()
            .unwrap()
            .to_owned())
    }

    /// exits the program with an error if client is not pointing at mongos process
    pub async fn assert_mongos(client: &mongodb::Client) -> mongodb::error::Result<()> {
        if !is_mongos(client).await {
            log::error!("Error: Must connect to mongos");
            std::process::exit(1);
        }
        Ok(())
    }

    pub async fn find_chunk_ids(
        client: &mongodb::Client,
        ns: &mongodb::Namespace,
        shard_key: &bson::Document,
        chunk: &Chunk,
    ) -> mongodb::Cursor<Id> {
        let index_hint = mongodb::options::Hint::Keys(shard_key.to_owned());
        let projection = bson::doc! { "_id": 1 };
        let options = mongodb::options::FindOptions::builder()
            .min(chunk.min.to_owned())
            .max(chunk.max.to_owned())
            .hint(index_hint)
            .projection(projection)
            .build();
        let result = client
            .database(&ns.db)
            .collection::<Id>(&ns.coll)
            .find(None, options)
            .await
            .expect("cannot execute find");
        result
    }
}
