mod chunk;
mod cli;
mod cluster;
mod db;
mod orphan;
mod util;

const BUFFER_SIZE: usize = 100_000;

fn init_logging() {
    let mut builder = env_logger::Builder::from_default_env();
    builder.target(env_logger::Target::Stdout);
    builder.init();
}

async fn estimate(
    cluster: cluster::ShardedCluster,
    ns: mongodb::Namespace,
) -> mongodb::error::Result<()> {
    let estimate = cluster.estimate_orphaned(&ns).await?;
    log::info!("estimated_count: {}", estimate);
    Ok(())
}

async fn print(
    cluster: cluster::ShardedCluster,
    ns: mongodb::Namespace,
    verbose: bool,
) -> mongodb::error::Result<()> {
    let orphans = cluster.find_orphaned(&ns).await?;
    log::trace!("{:?}", orphans);
    log::info!(
        "found {} orphans on {} shard(s): {:?}",
        orphans.cluster_total(),
        orphans.num_shards(),
        orphans.shard_totals(),
    );
    if verbose == true {
        log::info!("{:?}", orphans.shard_map());
    }
    Ok(())
}

async fn update(
    cluster: cluster::ShardedCluster,
    ns: mongodb::Namespace,
    target_ns: Option<String>,
) -> mongodb::error::Result<()> {
    log::debug!("target ns of {:?}", target_ns);
    if let Some(target) = target_ns {
        let target_ns = util::parse_ns(target.as_str());
        cluster.update_orphaned(&ns, Some(&target_ns)).await?;
    } else {
        cluster.update_orphaned(&ns, None).await?;
    }
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
        cli::Mode::Estimate => estimate(cluster, ns).await,
        cli::Mode::Print { verbose } => print(cluster, ns, verbose).await,
        cli::Mode::Update { target_ns } => update(cluster, ns, target_ns).await,
    }
}
