use std::{path::PathBuf, time::Duration};

use clap::{CommandFactory, Parser};
use monad_event_ring::{DecodedEventRing, EventNextResult};
use monad_exec_events::{
    BlockBuilderError, BlockCommitState, CommitStateBlockBuilder, CommitStateBlockUpdate,
    ExecEventRing,
};

#[derive(Debug, Parser)]
#[command(name = "monad-exec-events-blockdump", about, long_about = None)]
pub struct Cli {
    #[arg(long)]
    event_ring_path: PathBuf,

    #[arg(long)]
    filter_commit_state: Option<String>,

    #[arg(long, default_value_t = false)]
    json: bool,
}

fn main() {
    let Cli {
        event_ring_path,
        filter_commit_state,
        json,
    } = Cli::parse();

    if json && cfg!(not(feature = "alloy")) {
        Cli::command()
            .error(
                clap::error::ErrorKind::InvalidValue,
                "JSON requires compilation with \"alloy\" feature!",
            )
            .exit();
    }

    let filter_commit_state = filter_commit_state.map(|input| match input.as_str() {
        "proposed" => BlockCommitState::Proposed,
        "voted" => BlockCommitState::Voted,
        "finalized" => BlockCommitState::Finalized,
        "verified" => BlockCommitState::Verified,
        _ => panic!("unknown commit state"),
    });

    let event_ring = ExecEventRing::new_from_path(event_ring_path).unwrap();

    let mut event_reader = event_ring.create_reader();

    let mut block_builder = CommitStateBlockBuilder::default();

    loop {
        let event = match event_reader.next_descriptor() {
            EventNextResult::Gap => panic!("event ring gapped"),
            EventNextResult::NotReady => {
                std::thread::sleep(Duration::from_millis(1));
                continue;
            }
            EventNextResult::Ready(event) => event,
        };

        let Some(result) = block_builder.process_event_descriptor(&event) else {
            continue;
        };

        match result {
            Err(BlockBuilderError::Rejected) => panic!("execution rejected block"),
            Err(BlockBuilderError::PayloadExpired) => panic!("event ring payload expired"),
            Err(BlockBuilderError::ImplicitDrop { .. }) => unreachable!(),
            Ok(CommitStateBlockUpdate {
                block,
                state,
                abandoned: _,
            }) => {
                if let Some(filter_commit_state) = filter_commit_state {
                    if filter_commit_state != state {
                        continue;
                    }
                }

                if json {
                    #[cfg(not(feature = "alloy"))]
                    unreachable!();
                    #[cfg(feature = "alloy")]
                    {
                        let alloy_block = block.to_alloy_rpc();
                        println!("{}", serde_json::to_string(&alloy_block).unwrap());
                    }
                } else {
                    println!("{:?}", block);
                }
            }
        }
    }
}
