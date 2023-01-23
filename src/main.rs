mod cli;
mod db;
mod orphanage;
mod sharding;
mod util;

#[tokio::main]
async fn main() -> mongodb::error::Result<()> {
    let args = cli::args();
    let ns = db::Namespace {
        db: args.db.as_str(),
        coll: args.coll.as_str(),
    };

    let mongos = db::connect(&args.uri).await?;
    db::assert_mongos(&mongos).await?;
    let shards = db::connect_to_shards(&mongos).await?;

    let _orphans = orphanage::find_orphaned(mongos, shards, ns).await?;
    Ok(())
}
