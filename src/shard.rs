use mongodb::bson;

use crate::{chunk::Chunk, db};

#[derive(Debug, Clone)]
pub struct Shard {
    pub name: String,
    client: mongodb::Client,
}

impl Shard {
    pub fn new(name: String, client: mongodb::Client) -> Self {
        Self {
            name,
            client: client.clone(),
        }
    }

    pub async fn find_chunk_doc_ids(
        &self,
        ns: &mongodb::Namespace,
        shard_key: bson::Document,
        chunk: Chunk,
    ) {
        let mut chunk_ids = db::mongos::find_chunk_ids(&self.client, &ns, &shard_key, &chunk).await;
        while chunk_ids
            .advance()
            .await
            .expect("cannot advance chunk id cursor")
        {
            let id = chunk_ids
                .deserialize_current()
                .expect("cannot read doc from cursor");
            log::trace!("{:?}", &id);
        }
    }
}
