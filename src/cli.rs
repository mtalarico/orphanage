use clap::Parser;

/// Simple program to greet a person
#[derive(Parser)]
#[command(author="Michael T", version = "1.0", about="Find orphan documents on your sharded MongoDB cluster", long_about = None)]
pub struct Args {
    /// URI of MongoDB cluster, refer to https://www.mongodb.com/docs/manual/reference/connection-string/ for format
    #[arg(long)]
    pub uri: String,

    /// Database name
    #[arg(short, long)]
    pub db: String,

    /// Collection name
    #[arg(short, long)]
    pub coll: String,

    /// Print individual IDs of each orphan doc in each shard -- *warning* this may produce signficant output
    #[arg(long)]
    pub verbose: bool,
}

pub fn args() -> Args {
    Args::parse()
}
