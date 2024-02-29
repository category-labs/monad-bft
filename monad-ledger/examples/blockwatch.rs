use std::{
    fs::File,
    io::{self, Read},
    path::{Path, PathBuf},
};

use alloy_rlp::Decodable;
use clap::Parser;
use monad_crypto::hasher::{Blake3Hash, Hash, Hasher};
use notify::{
    event::{AccessKind, AccessMode},
    Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher,
};
use reth_primitives::{Block, B256};
use tokio::sync::mpsc;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    ledger_path: PathBuf,

    // Load a single block
    #[arg(long)]
    debug: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let args = Args::parse();

    // debug a single block
    if let Some(debug_path) = args.debug {
        let (seq_num, cnt_delta, hash) = process_block(&debug_path);
        println!(
            "seqnum: {:?}, tx count: {}, running hash: {:?}",
            seq_num, cnt_delta, hash
        );
        return Ok(());
    }

    if let Err(e) = async_watch(args.ledger_path).await {
        println!("error: {:?}", e)
    }

    Ok(())
}

fn async_watcher() -> notify::Result<(RecommendedWatcher, mpsc::Receiver<notify::Result<Event>>)> {
    let (tx, rx) = mpsc::channel(1);

    let watcher = RecommendedWatcher::new(
        move |res| {
            futures::executor::block_on(async {
                tx.send(res).await.unwrap();
            })
        },
        Config::default(),
    )?;

    Ok((watcher, rx))
}

async fn async_watch<P: AsRef<Path>>(path: P) -> notify::Result<()> {
    let (mut watcher, mut rx) = async_watcher()?;

    // Add a path to be watched. All files and directories at that path and
    // below will be monitored for changes.
    watcher.watch(path.as_ref(), RecursiveMode::Recursive)?;

    let mut running_txn_cnt = 0_u64;
    let mut running_hash = Hash::default();

    while let Some(res) = rx.recv().await {
        let mut hasher = Blake3Hash::new();
        hasher.update(running_hash);

        match res {
            Ok(Event {
                kind,
                paths,
                attrs: _,
            }) if kind == EventKind::Access(AccessKind::Close(AccessMode::Write)) => {
                assert_eq!(paths.len(), 1);
                let (seq_num, cnt_delta, hash) = process_block(&paths[0]);
                running_txn_cnt += cnt_delta;
                hasher.update(hash);
                running_hash = hasher.hash();
                println!(
                    "seqnum: {:?}, tx count: {}, running hash: {}",
                    seq_num, running_txn_cnt, running_hash
                );
            }
            Ok(_) => (),
            Err(e) => println!("watch error: {:?}", e),
        }
    }

    Ok(())
}

fn process_block(path: &PathBuf) -> (u64, u64, Hash) {
    let mut f = File::open(path).unwrap();

    let sz = f.metadata().unwrap().len();
    let mut buf = vec![0; sz as usize];
    f.read_exact(&mut buf).unwrap();

    let b = Block::decode(&mut &buf[..]);
    match b {
        Ok(block) => {
            // get data on the block
            let cnt = block.body.len() as u64;
            let (h, block) = block.seal(B256::default()).split_header_body();
            let tx_hashes = block.calculate_tx_root();
            (h.header.number, cnt, Hash(*tx_hashes))
        }
        Err(e) => {
            panic!("block decode failed: {}", e);
        }
    }
}
