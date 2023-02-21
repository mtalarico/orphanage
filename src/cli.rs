use clap::{Parser, Subcommand};

/// Simple program to greet a person
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// URI of MongoDB cluster, refer to https://www.mongodb.com/docs/manual/reference/connection-string/ for format
    #[arg(long, default_value = "mongodb://localhost:27016")]
    pub uri: String,

    /// Database name
    #[arg(short, long, default_value = "test")]
    pub db: String,

    /// Collection name
    #[arg(short, long, default_value = "test")]
    pub coll: String,

    #[command(subcommand)]
    pub mode: Mode,
}

#[derive(Subcommand)]
pub enum Mode {
    /// Return each shard's orphan count based on metadata [lowest performance impact]
    Estimate,
    /// Query each shard's real orphan count or list of IDs [heavier performance impact]
    Print {
        /// Set true to print a verbose map of each shard's orphan ID
        #[arg(long, default_value_t = false)]
        verbose: bool,
    },
    /// Query and update each shard, marking its orphans or writing their IDs to a designated namespace [heaviest performance impact]
    Update {
        /// instead of updating in place, write orphan IDs to a new namespace
        #[arg(long = "ns")]
        target_ns: Option<String>,
    },
}

pub fn args() -> Args {
    Args::parse()
}
