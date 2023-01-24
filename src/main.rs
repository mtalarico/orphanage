use tokio::time;

mod chunk;
mod cli;
mod cluster;
mod db;
mod shard;
mod util;

#[tokio::main]
async fn main() -> mongodb::error::Result<()> {
    let args = cli::args();
    let ns = mongodb::Namespace {
        db: args.db,
        coll: args.coll,
    };

    let cluster = cluster::ShardedCluster::new(&args.uri).await?;
    cluster.find_orphaned(&ns).await?;

    time::sleep(time::Duration::from_secs(10)).await;
    Ok(())
}
