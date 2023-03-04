mod cli;
mod cluster;
mod db;
mod util;

fn init_logging() {
    let mut builder = env_logger::Builder::from_default_env();
    builder.target(env_logger::Target::Stdout);
    builder.init();
}

async fn count(
    cluster: cluster::ShardedCluster,
    ns: mongodb::Namespace,
) -> mongodb::error::Result<()> {
    let orphans = cluster.count_orphaned(&ns).await?;
    println!("counted orphan: {:?}", orphans);
    Ok(())
}

#[tokio::main]
async fn main() -> mongodb::error::Result<()> {
    init_logging();

    let args = cli::args();

    let cluster = cluster::ShardedCluster::new(&args.uri).await?;

    let ns = mongodb::Namespace {
        db: args.db,
        coll: args.coll,
    };

    match args.mode {
        cli::Mode::Count => count(cluster, ns).await,
    }
}
