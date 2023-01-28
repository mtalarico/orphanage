use clap::Parser;

/// Simple program to greet a person
#[derive(Parser)]
#[command(author="Michael T", version = "1.0", about="For docs that were left at the doorstep", long_about = None)]
pub struct Args {
    /// URI of MongoDB cluster, either including credentials or not
    #[arg(long)]
    pub uri: String,

    /// Database name
    #[arg(short, long)]
    pub db: String,

    /// Collection name
    #[arg(short, long)]
    pub coll: String,

    /// Verbose
    #[arg(short, long)]
    pub verbose: bool,
}

pub fn args() -> Args {
    Args::parse()
}
