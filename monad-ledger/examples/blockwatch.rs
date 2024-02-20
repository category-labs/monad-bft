use std::{
    fs::File,
    io::{self, Read, Seek},
    path::{Path, PathBuf},
};

use alloy_rlp::Decodable;
use clap::Parser;
use heed::{types::SerdeBincode, Database, EnvOpenOptions};
use monad_crypto::hasher::{Blake3Hash, Hash, Hasher};
use notify::{
    event::{AccessKind, AccessMode},
    Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher,
};
use reth_primitives::{Block, B256};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    ledger_path: PathBuf,

    #[arg(short, long)]
    blockdb_path: PathBuf,

    // Load a single block
    #[arg(long)]
    debug: Option<PathBuf>,

    #[arg(long)]
    get_blockdb_key: Option<B256>,
}

const KECCAK_HDR_LEN: u64 = 32;
const ETH_TXN_HASH_DB: &str = "ethtxnhashdb";

type DatabaseType = Database<SerdeBincode<EthTxKey>, SerdeBincode<EthTxData>>;

#[derive(Debug, Serialize, Deserialize)]
struct EthTxKey(B256);

#[derive(Debug, Serialize, Deserialize)]
struct EthTxData {
    block_hash: B256,
    block_num: u64,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let args = Args::parse();

    // debug a single block
    if let Some(debug_path) = args.debug {
        let (seq_num, cnt_delta, hash) = process_block(&debug_path, None);
        println!(
            "seqnum: {:?}, tx count: {}, running hash: {:?}",
            seq_num, cnt_delta, hash
        );
        return Ok(());
    }

    // debug blockdb
    if let Some(key) = args.get_blockdb_key {
        let ethkey = EthTxKey(key);
        let blockdb_env = EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024)
            .max_dbs(3)
            .open(args.blockdb_path)
            .expect("db failed");
        let db: DatabaseType = blockdb_env
            .open_database(Some(ETH_TXN_HASH_DB))
            .expect("db should exist")
            .unwrap();

        let db_txn = blockdb_env.write_txn().expect("db txn create failed");
        let data: Option<EthTxData> = db.get(&db_txn, &ethkey).unwrap();

        println!("found: {:?}", data);

        return Ok(());
    }

    // create the block db stuff
    std::fs::create_dir_all(&args.blockdb_path)?;
    let blockdb_env = EnvOpenOptions::new()
        .map_size(10 * 1024 * 1024)
        .max_dbs(3)
        .open(args.blockdb_path)
        .expect("db failed");
    let _db: DatabaseType = blockdb_env.create_database(Some(ETH_TXN_HASH_DB)).unwrap();

    if let Err(e) = async_watch(args.ledger_path, Some(blockdb_env)).await {
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

async fn async_watch<P: AsRef<Path>>(
    path: P,
    blockdb_env: Option<heed::Env>,
) -> notify::Result<()> {
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
                let (seq_num, cnt_delta, hash) = process_block(&paths[0], blockdb_env.clone());
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

fn process_block(path: &PathBuf, blockdb_env: Option<heed::Env>) -> (u64, u64, Hash) {
    let mut f = File::open(path).unwrap();

    let sz = f.metadata().unwrap().len() - KECCAK_HDR_LEN;
    f.seek(io::SeekFrom::Start(KECCAK_HDR_LEN)).unwrap();

    let mut buf = vec![0; sz as usize];
    f.read_exact(&mut buf).unwrap();

    let b = Block::decode(&mut &buf[..]);
    match b {
        Ok(block) => {
            // spawn write to db thread
            if let Some(env) = blockdb_env {
                let db_block = block.clone();
                tokio::task::spawn_blocking(move || {
                    let db: DatabaseType = env
                        .open_database(Some(ETH_TXN_HASH_DB))
                        .expect("db should exist")
                        .unwrap();
                    let mut db_txn = env.write_txn().expect("db txn create failed");

                    for eth_tx in &db_block.body {
                        let key = EthTxKey(eth_tx.recalculate_hash());
                        let value = EthTxData {
                            block_hash: db_block.header.hash_slow(),
                            block_num: db_block.number,
                        };
                        db.put(&mut db_txn, &key, &value).expect("db put failed");
                    }

                    db_txn.commit().expect("db commit failed");
                });
            }

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
