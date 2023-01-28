mod chunk;
mod cli;
mod cluster;
mod db;
mod orphan;
mod shard;
mod util;

const BUFFER_SIZE: usize = 100_000;

fn init_logging() {
    let mut builder = env_logger::Builder::from_default_env();
    builder.target(env_logger::Target::Stdout);
    builder.init();
}

#[tokio::main]
async fn main() -> mongodb::error::Result<()> {
    init_logging();

    let args = cli::args();
    let ns = mongodb::Namespace {
        db: args.db,
        coll: args.coll,
    };

    // TODO larger channel size
    let cluster = cluster::Sharded::new(&args.uri).await?;
    let orphans = cluster.find_orphaned(&ns).await?;

    // log::trace!("{:?}", orphans);
    log::info!(
        "{:?}, total_count: {}",
        orphans.get_shard_totals(),
        orphans.get_cluster_total()
    );
    if args.verbose {
        log::info!("{:?}", orphans.get_shard_map());
    }

    Ok(())
}
