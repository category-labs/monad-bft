use std::path::{Path, PathBuf};

use clap::Parser;
use glob::glob;
use monad_consensus_types::checkpoint::Checkpoint;
use monad_node::config::SignatureCollectionType;
use monad_types::{Round, SeqNum};

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    forkpoints: PathBuf,
}

#[derive(Debug)]
enum Error {
    IOError(std::io::Error),
    CustomError(std::string::String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::IOError(error) => write!(f, "IOError {}", error),
            Error::CustomError(s) => write!(f, "CustomError {}", s),
        }
    }
}

impl std::error::Error for Error {}

type Result<T> = std::result::Result<T, Error>;

fn load_checkpoint(path: &Path) -> Result<Checkpoint<SignatureCollectionType>> {
    let checkpoint_raw = std::fs::read_to_string(path).map_err(Error::IOError)?;
    toml::from_str(&checkpoint_raw).map_err(|e| Error::CustomError(format!("{}", e)))
}

fn get_checkpoint_match_high_qc(
    round: Round,
    seq_num: SeqNum,
    forkpoints: &Path,
) -> Result<Checkpoint<SignatureCollectionType>> {
    let high_qc_pattern = format!("forkpoint.toml.*.{}", round.0);

    let full_pattern = forkpoints.join(high_qc_pattern);

    for entry in glob(
        full_pattern
            .to_str()
            .expect("converting pathbuf to string success"),
    )
    .map_err(|e| Error::CustomError(format!("{}", e)))?
    {
        let path = entry.map_err(|e| Error::CustomError(format!("{}", e)))?;
        let checkpoint = load_checkpoint(&path)?;
        if checkpoint.high_qc.get_round() == round && checkpoint.high_qc.get_seq_num() == seq_num {
            return Ok(checkpoint);
        }
    }
    Err(Error::CustomError(
        "No matching forkpoint high_qc".to_owned(),
    ))
}

fn main() -> Result<()> {
    let args = Args::parse();

    if !args.forkpoints.is_dir() {
        return Err(Error::CustomError(format!(
            "Invalid path {:?}",
            args.forkpoints
        )));
    }
    let ledger_tip = load_checkpoint(&args.forkpoints.join("forkpoint.toml"))?.root;
    let round = ledger_tip.round;
    let seq_num = ledger_tip.seq_num;

    let mut forkpoint_base = get_checkpoint_match_high_qc(round, seq_num, &args.forkpoints)?;

    if ledger_tip.block_id != forkpoint_base.high_qc.get_block_id() {
        return Err(Error::CustomError(
            "root and high_qc block_id mismatch".to_owned(),
        ));
    }

    // Use validator set from high_qc matched forkpoint. high_qc is guaranteed
    // to verify. If seq_num is close to epoch start, monad-bft will fail to
    // validate forkpoint. Need to manually fetch validator set from forkpoints
    // or execution
    forkpoint_base.root = ledger_tip;

    // change old filename to forkpoint.bak
    let forkpoint_bak = args.forkpoints.join("forkpoint.toml.bak");
    if forkpoint_bak.exists() {
        return Err(Error::CustomError("forkpoint.toml.bak exists".to_owned()));
    }

    let forkpoint_path = args.forkpoints.join("forkpoint.toml");
    if forkpoint_path.exists() {
        std::fs::rename(&forkpoint_path, forkpoint_bak).map_err(Error::IOError)?;
        println!("renamed old forkpoint.toml to forkpoint.toml.bak")
    }

    let forkpoint_str = toml::to_string_pretty(&forkpoint_base)
        .map_err(|e| Error::CustomError(format!("{}", e)))?;

    std::fs::write(forkpoint_path, forkpoint_str).map_err(Error::IOError)?;

    println!(
        "Forkpoint reset to seq_num={} round={} block_id={}",
        seq_num.0, round.0, forkpoint_base.root.block_id.0
    );
    Ok(())
}
