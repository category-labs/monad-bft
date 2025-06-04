use clap::{Parser, Subcommand};
use monad_archive::cli::ArchiveArgs;

#[derive(Debug, Parser)]
#[command(
    name = "monad-archive-checker",
    about = "Archive consistency checker for validating blockchain data across multiple replicas",
    long_about = "Archive consistency checker for validating blockchain data across multiple replicas.\n\n\
EXAMPLES:\n\n\
  # Start main checker with 3 replicas (runs continuously)\n\
  monad-archive-checker --bucket checker-state --region us-east-1 checker \\\n\
    --init-replicas 'aws archive-1 20,aws archive-2 20,aws archive-3 20'\n\n\
  # Run standalone rechecker to fix false positives (runs periodically)\n\
  monad-archive-checker --bucket checker-state rechecker --recheck-freq-min 5\n\n\
  # Inspect specific faults\n\
  monad-archive-checker --bucket checker-state inspector list-faults\n\
  monad-archive-checker --bucket checker-state inspector inspect-block 12345 --format all\n\n\
  # Fix faults by copying from good replicas (dry run first)\n\
  monad-archive-checker --bucket checker-state fault-fixer\n\
  monad-archive-checker --bucket checker-state fault-fixer --commit-changes --verify\n\n\
  # Advanced: Recheck specific block range with dry run\n\
  monad-archive-checker --bucket checker-state rechecker \\\n\
    --start-block 1000 --end-block 5000 --dry-run\n\n\
TYPICAL WORKFLOW:\n\
  1. Run 'checker' mode continuously to find inconsistencies\n\
  2. Use 'inspector' to analyze specific faults\n\
  3. Run 'rechecker' to eliminate false positives\n\
  4. Use 'fault-fixer' to repair confirmed issues"
)]
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
    /// Main checker mode - continuously validates blocks across replicas
    Checker(CheckerArgs),
    /// Standalone rechecker - rechecks fault chunks from scratch
    Rechecker(Rechecker),
    /// Repairs faults by copying data from good replicas
    FaultFixer(FaultFixerArgs),
    /// Inspects and analyzes fault data
    Inspector(InspectorArgs),
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

    /// Use rechecker v2 (full chunk recheck from scratch)
    #[arg(long)]
    pub use_rechecker_v2: bool,
}

#[derive(Parser, Debug)]
pub struct Rechecker {
    /// How frequently to recheck faults in minutes
    #[arg(long, default_value_t = 5.)]
    pub recheck_freq_min: f64,

    /// Use rechecker v2 (full chunk recheck from scratch)
    #[arg(long)]
    pub use_rechecker_v2: bool,

    /// Dry run mode - only print differences without updating faults
    #[arg(long)]
    pub dry_run: bool,

    /// Optional start block to recheck (inclusive)
    #[arg(long)]
    pub start_block: Option<u64>,

    /// Optional end block to recheck (inclusive)
    #[arg(long)]
    pub end_block: Option<u64>,
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

#[derive(Parser, Debug)]
pub struct InspectorArgs {
    #[command(subcommand)]
    pub command: InspectorCommand,
}

#[derive(Subcommand, Debug)]
pub enum InspectorCommand {
    /// List all fault ranges collapsed to start-end format
    ListFaults,

    /// List all blocks with faults in a given range
    ListFaultyBlocks {
        /// Start block (inclusive)
        #[arg(long)]
        start: Option<u64>,

        /// End block (inclusive)
        #[arg(long)]
        end: Option<u64>,
    },

    /// Inspect a specific block across all replicas
    InspectBlock {
        /// Block number to inspect
        block_num: u64,

        /// Output format
        #[arg(long, default_value = "summary")]
        format: InspectorOutputFormat,

        /// Print full parsed data
        #[arg(long)]
        print_data: bool,
    },
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum InspectorOutputFormat {
    /// Show all replicas
    All,
    /// Show only replicas with faults
    FaultsOnly,
    /// Show only the good replica
    GoodOnly,
    /// Show summary statistics only
    Summary,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_rechecker_v1_rejects_v2_options() {
        // Test that v1 rechecker rejects dry-run option
        let args = vec![
            "monad-archive-checker",
            "--bucket",
            "test-bucket",
            "rechecker",
            "--dry-run",
        ];

        let cli_result = Cli::try_parse_from(args);
        assert!(cli_result.is_ok(), "CLI should parse correctly");

        let cli = cli_result.unwrap();
        if let Mode::Rechecker(rechecker_args) = cli.mode {
            assert!(!rechecker_args.use_rechecker_v2);
            assert!(rechecker_args.dry_run);
            // The main.rs will validate this and return an error
        } else {
            panic!("Expected Rechecker mode");
        }
    }

    #[test]
    fn test_cli_rechecker_v2_accepts_all_options() {
        let args = vec![
            "monad-archive-checker",
            "--bucket",
            "test-bucket",
            "rechecker",
            "--use-rechecker-v2",
            "--dry-run",
            "--start-block",
            "1000",
            "--end-block",
            "2000",
            "--recheck-freq-min",
            "10",
        ];

        let cli = Cli::try_parse_from(args).expect("CLI should parse correctly");

        if let Mode::Rechecker(rechecker_args) = cli.mode {
            assert!(rechecker_args.use_rechecker_v2);
            assert!(rechecker_args.dry_run);
            assert_eq!(rechecker_args.start_block, Some(1000));
            assert_eq!(rechecker_args.end_block, Some(2000));
            assert_eq!(rechecker_args.recheck_freq_min, 10.0);
        } else {
            panic!("Expected Rechecker mode");
        }
    }

    #[test]
    fn test_cli_checker_mode_with_rechecker_v2() {
        let args = vec![
            "monad-archive-checker",
            "--bucket",
            "test-bucket",
            "checker",
            "--use-rechecker-v2",
            "--min-lag-from-tip",
            "1000",
        ];

        let cli = Cli::try_parse_from(args).expect("CLI should parse correctly");

        if let Mode::Checker(checker_args) = cli.mode {
            assert!(checker_args.use_rechecker_v2);
            assert_eq!(checker_args.min_lag_from_tip, 1000);
        } else {
            panic!("Expected Checker mode");
        }
    }

    #[test]
    fn test_cli_inspector_commands() {
        // Test list-faults command
        let args = vec![
            "monad-archive-checker",
            "--bucket",
            "test-bucket",
            "inspector",
            "list-faults",
        ];

        let cli = Cli::try_parse_from(args).expect("CLI should parse correctly");

        if let Mode::Inspector(inspector_args) = cli.mode {
            match inspector_args.command {
                InspectorCommand::ListFaults => {}
                _ => panic!("Expected ListFaults command"),
            }
        } else {
            panic!("Expected Inspector mode");
        }

        // Test inspect-block command
        let args = vec![
            "monad-archive-checker",
            "--bucket",
            "test-bucket",
            "inspector",
            "inspect-block",
            "12345",
            "--format",
            "faults-only",
            "--print-data",
        ];

        let cli = Cli::try_parse_from(args).expect("CLI should parse correctly");

        if let Mode::Inspector(inspector_args) = cli.mode {
            match inspector_args.command {
                InspectorCommand::InspectBlock {
                    block_num,
                    format,
                    print_data,
                } => {
                    assert_eq!(block_num, 12345);
                    assert!(matches!(format, InspectorOutputFormat::FaultsOnly));
                    assert!(print_data);
                }
                _ => panic!("Expected InspectBlock command"),
            }
        } else {
            panic!("Expected Inspector mode");
        }
    }

    #[test]
    fn test_cli_fault_fixer_mode() {
        let args = vec![
            "monad-archive-checker",
            "--bucket",
            "test-bucket",
            "fault-fixer",
            "--commit-changes",
            "--verify",
            "--replicas",
            "replica1,replica2",
        ];

        let cli = Cli::try_parse_from(args).expect("CLI should parse correctly");

        if let Mode::FaultFixer(fixer_args) = cli.mode {
            assert!(fixer_args.commit_changes);
            assert!(fixer_args.verify);
            assert_eq!(
                fixer_args.replicas,
                Some(vec!["replica1".to_string(), "replica2".to_string()])
            );
        } else {
            panic!("Expected FaultFixer mode");
        }
    }
}
