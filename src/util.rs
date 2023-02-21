use mongodb::bson;

use crate::db;

pub fn parse_ns(ns: &str) -> mongodb::Namespace {
    let split: Vec<&str> = ns.split(".").collect();
    if split.len() != 2 {
        panic!("malformed namespace");
    }
    mongodb::Namespace {
        db: String::from(split[0]),
        coll: String::from(split[1]),
    }
}

/// Convert internal connection string format (rs/host:port,...,host:port) to uri format (mongodb://[username:password@]host1[:port1][,...hostN[:portN]][/[defaultauthdb][?options]])
pub fn update_connection_string(cluster: &str, shard: &str) -> String {
    let hosts = get_host_from_connection_string(cluster);
    let shard_nodes = shard.split('/').nth(1).unwrap();
    let mut result = cluster.clone().replace(hosts, shard_nodes);

    // mongos needs to use an alias localThreshold, if that is the case replace the alias
    if cluster.contains("localThreshold") {
        result = result.replace("localThreshold", "localThresholdMS");
    }

    // we are not connecting over srv to the shards, remove it from the new uri
    if cluster.contains("+srv") {
        result = result.replace("+srv", "");
    }

    result
}

fn get_host_from_connection_string(uri: &str) -> &str {
    let cut_left: &str;
    // if they provided inline auth, split after that -- else take after the protocol
    if uri.contains('@') {
        cut_left = uri.split("@").nth(1).expect("there is nothing after auth?");
    } else {
        cut_left = uri
            .split("://")
            .nth(1)
            .expect("there is nothing after protocol?");
    }
    let cut_right = cut_left
        .split("/")
        .nth(0)
        .expect("theres nothing before authdb/parameters?");
    cut_right
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

#[cfg(test)]
mod tests {

    #[test]
    fn convert_plain_connection_string() {
        let cluster = "mongodb://localhost:27016";
        let shard = "shard01/localhost:27017,localhost:27018,localhost:27019";
        let expected = "mongodb://localhost:27017,localhost:27018,localhost:27019";
        let actual = super::update_connection_string(cluster, shard);
        assert_eq!(expected, actual);
    }

    #[test]
    fn convert_connection_string_options() {
        let cluster = "mongodb://localhost:27016/?readPreference=secondary&w=majority";
        let shard = "shard01/localhost:27017,localhost:27018,localhost:27019";
        let expected = "mongodb://localhost:27017,localhost:27018,localhost:27019/?readPreference=secondary&w=majority";
        let actual = super::update_connection_string(cluster, shard);
        assert_eq!(expected, actual);
    }

    #[test]
    fn convert_connection_string_auth() {
        let cluster = "mongodb://test:test@localhost:27016";
        let shard = "shard01/localhost:27017,localhost:27018,localhost:27019";
        let expected = "mongodb://test:test@localhost:27017,localhost:27018,localhost:27019";
        let actual = super::update_connection_string(cluster, shard);
        assert_eq!(expected, actual);
    }

    #[test]
    fn convert_connection_string_auth_and_options() {
        let cluster = "mongodb://test:test@localhost:27016/?readPreference=secondary&w=majority";
        let shard = "shard01/localhost:27017,localhost:27018,localhost:27019";
        let expected = "mongodb://test:test@localhost:27017,localhost:27018,localhost:27019/?readPreference=secondary&w=majority";
        let actual = super::update_connection_string(cluster, shard);
        assert_eq!(expected, actual);
    }

    #[test]
    fn convert_connection_string_auth_and_default_db_and_options() {
        let cluster =
            "mongodb://test:test@localhost:27016/admin?readPreference=secondary&w=majority";
        let shard = "shard01/localhost:27017,localhost:27018,localhost:27019";
        let expected = "mongodb://test:test@localhost:27017,localhost:27018,localhost:27019/admin?readPreference=secondary&w=majority";
        let actual = super::update_connection_string(cluster, shard);
        assert_eq!(expected, actual);
    }

    #[test]
    fn convert_srv_conn_str() {
        let cluster =
            "mongodb+srv://test:test@playground.ah123.mongodb.net/admin?readPreference=secondary&w=majority";
        let shard = "shard01/somehost1:27017,somehost2:27018,somehost3:27019";
        let expected = "mongodb://test:test@somehost1:27017,somehost2:27018,somehost3:27019/admin?readPreference=secondary&w=majority";
        let actual = super::update_connection_string(cluster, shard);
        assert_eq!(expected, actual);
    }
}
