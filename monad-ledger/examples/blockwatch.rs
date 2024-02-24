use std::{
    fs::File,
    io::{self, Read, Seek},
    path::{Path, PathBuf},
};

use alloy_rlp::Decodable;
use clap::Parser;
use heed::{Env, EnvOpenOptions};
use monad_blockdb::{
    BlockNumTableKey, BlockNumTableType, BlockTableKey, BlockTableType, BlockTagKey,
    BlockTagTableType, BlockTagValue, BlockValue, EthTxKey, EthTxValue, TxnHashTableType,
    BLOCK_DB_MAP_SIZE, BLOCK_DB_NUM_DBS, BLOCK_NUM_TABLE_NAME, BLOCK_TABLE_NAME,
    BLOCK_TAG_TABLE_NAME, TXN_HASH_TABLE_NAME,
};
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

    #[arg(short, long)]
    blockdb_path: PathBuf,

    // Load a single block
    #[arg(long)]
    debug: Option<PathBuf>,

    #[arg(long)]
    get_blockdb_key: Option<B256>,
}

const KECCAK_HDR_LEN: u64 = 32;

fn create_tables(blockdb_path: &Path) -> io::Result<Env> {
    std::fs::create_dir_all(blockdb_path)?;
    let blockdb_env = EnvOpenOptions::new()
        .map_size(BLOCK_DB_MAP_SIZE)
        .max_dbs(BLOCK_DB_NUM_DBS)
        .open(blockdb_path)
        .expect("db failed");

    let _: BlockTableType = blockdb_env.create_database(Some(BLOCK_TABLE_NAME)).unwrap();
    let _: BlockNumTableType = blockdb_env
        .create_database(Some(BLOCK_NUM_TABLE_NAME))
        .unwrap();
    let _: TxnHashTableType = blockdb_env
        .create_database(Some(TXN_HASH_TABLE_NAME))
        .unwrap();
    let _: BlockTagTableType = blockdb_env
        .create_database(Some(BLOCK_TAG_TABLE_NAME))
        .unwrap();

    Ok(blockdb_env)
}

fn update_tables(block: Block, blockdb_env: Env) {
    let block_hash = BlockTableKey(block.header.hash_slow());

    // put txn hashes into table
    let txn_hash_table: TxnHashTableType = blockdb_env
        .open_database(Some(TXN_HASH_TABLE_NAME))
        .expect("txn_hash_table should exist")
        .unwrap();
    let mut txn_hash_table_txn = blockdb_env
        .write_txn()
        .expect("txn_hash_table txn create failed");

    for (i, eth_tx) in block.body.iter().enumerate() {
        let key = EthTxKey(eth_tx.recalculate_hash());
        let value = EthTxValue {
            block_hash: block_hash.clone(),
            transaction_index: i as u64,
        };
        txn_hash_table
            .put(&mut txn_hash_table_txn, &key, &value)
            .expect("txn_hash_table put failed");
    }
    txn_hash_table_txn
        .commit()
        .expect("txn_hash_table commit failed");

    // put block numbers into table
    let block_num_table: BlockNumTableType = blockdb_env
        .open_database(Some(BLOCK_NUM_TABLE_NAME))
        .expect("block_num_table should exist")
        .unwrap();
    let mut block_num_table_txn = blockdb_env
        .write_txn()
        .expect("block_num_table txn create failed");
    let block_num_table_key = BlockNumTableKey(block.number);
    let block_num_table_value = block_hash.clone();
    block_num_table
        .put(
            &mut block_num_table_txn,
            &block_num_table_key,
            &block_num_table_value,
        )
        .expect("block_num_table put failed");
    block_num_table_txn
        .commit()
        .expect("block_num_table commit failed");

    // put full block into table
    let block_table: BlockTableType = blockdb_env
        .open_database(Some(BLOCK_TABLE_NAME))
        .expect("block_table should exist")
        .unwrap();
    let mut block_table_txn = blockdb_env
        .write_txn()
        .expect("block_table txn create failed");
    let block_table_key = block_hash.clone();
    let block_table_value = BlockValue { block };
    block_table
        .put(&mut block_table_txn, &block_table_key, &block_table_value)
        .expect("block_table put failed");
    block_table_txn.commit().expect("block_table commit failed");

    // update the blocktag table
    let block_tag_table: BlockTagTableType = blockdb_env
        .open_database(Some(BLOCK_TAG_TABLE_NAME))
        .expect("block_tag_table should exist")
        .unwrap();
    let mut block_tag_table_txn = blockdb_env
        .write_txn()
        .expect("block_tag_table txn create failed");
    let block_tag_value = BlockTagValue { block_hash };
    block_tag_table
        .put(
            &mut block_tag_table_txn,
            &BlockTagKey::Latest,
            &block_tag_value,
        )
        .expect("block_tag_table put failed");
    block_tag_table
        .put(
            &mut block_tag_table_txn,
            &BlockTagKey::Finalized,
            &block_tag_value,
        )
        .expect("block_tag_table put failed");
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
        let db: TxnHashTableType = blockdb_env
            .open_database(Some(TXN_HASH_TABLE_NAME))
            .expect("db should exist")
            .unwrap();

        let db_txn = blockdb_env.write_txn().expect("db txn create failed");
        let data: Option<EthTxValue> = db.get(&db_txn, &ethkey).unwrap();

        println!("found: {:?}", data);

        return Ok(());
    }

    let blockdb_env = create_tables(&args.blockdb_path)?;

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
            if let Some(env) = blockdb_env {
                update_tables(block.clone(), env);
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
