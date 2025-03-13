use clap::{Parser, Subcommand};
use monad_archive::cli::ArchiveArgs;

#[derive(Debug, Parser)]
#[command(name = "monad-archive-checker", about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub mode: Mode,

    /// S3 bucket name for storing checker state
    #[arg(long)]
    pub bucket: String,

    /// AWS region
    #[arg(long)]
    pub region: Option<String>,

    #[arg(long)]
    pub otel_endpoint: Option<String>,

    #[arg(long)]
    pub max_compute_threads: Option<usize>,
}

#[derive(Subcommand, Debug)]
pub enum Mode {
    Checker(CheckerArgs),
    Rechecker(Rechecker),
    FaultFixer(FaultFixerArgs),
}

#[derive(Parser, Debug)]
pub struct CheckerArgs {
    /// Comma-separated list of replicas to check
    /// Format: 'aws bucket1 [concurrency1] [region1],aws bucket2 [concurrency2] [region2],...'
    #[arg(long, value_delimiter = ',', value_parser = clap::value_parser!(ArchiveArgs))]
    pub init_replicas: Option<Vec<ArchiveArgs>>,

    /// Flag to disable running rechecker worker to determine if existing faults still exist
    #[arg(long)]
    pub disable_rechecker: bool,

    /// The minimum difference between the block_num tip of the latest replica
    /// and the latest block to check
    /// E.g. if replica tips are 2000, 2100 and 2005, and  --min-lag-from-tip is 500,
    /// then we would check up to block_num max(2000,2100,2005) - 500 = 2100 - 500 = 1600
    #[arg(long, default_value_t = 1500)]
    pub min_lag_from_tip: u64,

    /// How frequently to recheck faults in minutes
    #[arg(long, default_value_t = 15.)]
    pub recheck_freq_min: f64,
}

#[derive(Parser, Debug)]
pub struct Rechecker {
    /// How frequently to recheck faults in minutes
    #[arg(long, default_value_t = 5.)]
    pub recheck_freq_min: f64,
}

#[derive(Parser, Debug)]
pub struct FaultFixerArgs {
    /// Commit changes to replicas
    /// Otherwise runs in dry-run mode
    #[clap(long)]
    pub commit_changes: bool,

    /// Verify fixed blocks after repair
    #[clap(long)]
    pub verify: bool,

    /// Comma-separated list of specific replicas to fix (defaults to all)
    #[clap(long, value_delimiter = ',')]
    pub replicas: Option<Vec<String>>,
}
