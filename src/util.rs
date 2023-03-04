use mongodb::bson;

pub fn make_projection(document: &bson::Document) -> bson::Document {
    let x = document
        .clone()
        .into_iter()
        .map(|(k, _)| (k, bson::Bson::Int32(1)));
    bson::Document::from_iter(x)
}

// /// Convert internal connection string format (rs/host:port,...,host:port) to uri format (mongodb://[username:password@]host1[:port1][,...hostN[:portN]][/[defaultauthdb][?options]])
// pub fn update_connection_string(cluster: &str, shard: &str) -> String {
//     let hosts = get_host_from_connection_string(cluster);
//     let shard_nodes = shard.split('/').nth(1).unwrap();
//     let mut result = cluster.clone().replace(hosts, shard_nodes);

//     // mongos needs to use an alias localThreshold, if that is the case replace the alias
//     if cluster.contains("localThreshold") {
//         result = result.replace("localThreshold", "localThresholdMS");
//     }

//     // we are not connecting over srv to the shards, remove it from the new uri
//     if cluster.contains("+srv") {
//         result = result.replace("+srv", "");
//     }

//     result
// }

// fn get_host_from_connection_string(uri: &str) -> &str {
//     let cut_left: &str;
//     // if they provided inline auth, split after that -- else take after the protocol
//     if uri.contains('@') {
//         cut_left = uri.split("@").nth(1).expect("there is nothing after auth?");
//     } else {
//         cut_left = uri
//             .split("://")
//             .nth(1)
//             .expect("there is nothing after protocol?");
//     }
//     let cut_right = cut_left
//         .split("/")
//         .nth(0)
//         .expect("theres nothing before authdb/parameters?");
//     cut_right
// }

/// There has to be a better way to do it, but this is the quick and dirty right now
pub fn isdbgrid_error(err: mongodb::error::Error) -> bool {
    return err.to_string().contains("no such command: 'isdbgrid'");
}
