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

pub fn version_above_5(version: &str) -> bool {
    let major_version = version.split(".").collect::<Vec<&str>>()[0];
    let major_version = major_version
        .parse::<i32>()
        .expect("cannot parse cluster version");
    major_version >= 5
}

pub async fn get_cluster_version(client: &mongodb::Client) -> mongodb::error::Result<String> {
    let doc = client
        .database("admin")
        .run_command(bson::doc! {"serverStatus": 1}, None)
        .await?;
    let version = doc.get_str("version").expect("cannot get server version");
    Ok(String::from(version))
}

pub async fn get_uuid_for_ns(
    client: &mongodb::Client,
    ns: &db::Namespace,
) -> mongodb::error::Result<bson::Binary> {
    let filter = bson::doc! { "_id": ns.as_str() };
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
