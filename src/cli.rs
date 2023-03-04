use clap::{Parser, Subcommand};

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
    /// Return each shard's orphan count via covered querys with shard key index
    Count,
}

pub fn args() -> Args {
    Args::parse()
}
