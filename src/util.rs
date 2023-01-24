use mongodb::bson;

use crate::db;

/// Convert internal connection string format (rs/host:port,...,host:port) to uri format (mongodb://host:port,...,host:port/?replicaSet=name)
pub fn update_connection_string(original: &str) -> String {
    let parts: Vec<&str> = original.split('/').collect();
    return ["mongodb://", parts[1], "/?replicaSet=", parts[0]].join("");
}

/// There has to be a better way to do it, but this is the quick and dirty right now
pub fn isdbgrid_error(err: mongodb::error::Error) -> bool {
    return err.to_string().contains("no such command: 'isdbgrid'");
}

pub async fn get_ns_filter(
    client: &mongodb::Client,
    ns: &mongodb::Namespace,
) -> mongodb::error::Result<bson::Document> {
    let version = db::get_version(&client).await?;

    if version_above_5(version.as_str()) == true {
        let uuid = get_uuid_for_ns(&client, &ns).await?;
        Ok(bson::doc! { "uuid": uuid })
    } else {
        Ok(bson::doc! {"ns": ns.to_string().as_str()})
    }
}

fn version_above_5(version: &str) -> bool {
    let major_version = version.split(".").collect::<Vec<&str>>()[0];
    let major_version = major_version
        .parse::<i32>()
        .expect("cannot parse cluster version");
    major_version >= 5
}

async fn get_uuid_for_ns(
    client: &mongodb::Client,
    ns: &mongodb::Namespace,
) -> mongodb::error::Result<bson::Binary> {
    let filter = bson::doc! { "_id": ns.to_string().as_str() };
    let doc = client
        .database("config")
        .collection::<bson::Document>("collections")
        .find_one(filter, None)
        .await?;
    let doc = doc.expect("no colleciton with that namespace");
    let uuid = doc
        .get("uuid")
        .expect("cannot parse uuid from collection document");
    match uuid {
        bson::Bson::Binary(bin) => Ok(bin.to_owned()),
        _ => panic!("uuid is not uuid type?"),
    }
}
