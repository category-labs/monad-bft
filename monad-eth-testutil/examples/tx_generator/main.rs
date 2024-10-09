mod final_accounts;
mod refresher;
mod rpc;

use std::{
    cell::OnceCell,
    error::Error,
    sync::{
        atomic::{AtomicU8, AtomicUsize, Ordering},
        Arc,
    },
};

use final_accounts::*;
use refresher::*;
use rpc::*;

use alloy_primitives::Bytes;
use async_channel::{Receiver, Sender};
use clap::{Parser, ValueEnum};
use futures::future::join_all;
use rand::{rngs::SmallRng, Rng, SeedableRng};
use reqwest::Url;
use tokio::time::{Duration, Instant};

// max reserve balance is hardcoded in execution to be 10^17 right now
// const MAX_RESERVE_BALANCE_PER_ACCOUNT: usize = 100_000_000_000_000_000;

pub const TXN_CARRIAGE_COST: usize = 21000 * 1000;
// for transfer transactions only
pub const TXN_INTRINSIC_GAS_FEE: usize = 32000 * 1000;
pub const TXN_GAS_FEES: usize = TXN_CARRIAGE_COST + TXN_INTRINSIC_GAS_FEE;

// This is the expected wait time after sending transactions before
// trying to refresh nonce and balance so that it is up to date
// NOTE: This is completely arbitrary, the time may vary depending on
// how fast execution runs on a specific node and network conditions
pub const EXECUTION_DELAY_WAIT_TIME: Duration = Duration::from_secs(10);

// number of batches of transactions allowed in the txn batches channel
pub const TX_BATCHES_CHANNEL_BUFFER: usize = 50;

// number of batches of accounts allowed in the pipeline
// 600 batches of 500,000 accounts = 300,000,000 accounts
pub const ACCOUNTS_BATCHES_CHANNEL_BUFFER: usize = 600;

// number of accounts to be created per accounts batch
// each accounts batch is sent into the pipeline
pub const NUM_ACCOUNTS_PER_BATCH: usize = 500_000;
// number of txn batches to generate per accounts batch
// 4,000 batches = 2,000,000 txns per accounts batch in pipeline
pub const NUM_TXN_BATCHES_PER_ACCOUNTS_BATCH: usize = 4_000;
// e.g. with 10,000,000 final accounts, there will be 20 accounts batches
// if each batch sends 2,000,000 txns, total txns count is 40,000,000 txns

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "http://localhost:8080")]
    rpc_url: Url,

    /// private key for initial account to source tokens to other accounts
    #[arg(
        long,
        default_value = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
    )]
    root_private_key: String,

    /// Initial splitting of root account balance occurs in batches. Balance of root account
    /// is split into multiple accounts and balance of newly created accounts are split further.
    /// May create more accounts than num_final_accounts if not exactly divisible by BATCH_SIZE
    #[arg(long, default_value_t = 1_000_000)]
    num_final_accounts: u64,

    /// if true, only generates the final accounts (useful for creating DB snapshots and priv keys file)
    #[arg(long)]
    final_accs_only: bool,

    /// if true, loads the private keys from the "priv_keys_file". panics if true and file not provided
    #[arg(long)]
    load_final_accs: bool,

    /// if provided and load_final_accs is true, will load from this file
    /// if provided and load_final_accs is false, will store newly created priv keys in this file
    #[arg(long)]
    priv_keys_file: Option<String>,

    // Payload size with batch = 868 raw txns: 262144
    // Payload size with batch = 869 raw txns: 262408 -> payload reached size limit
    // IPC batch size limit = 500
    #[arg(long, default_value_t = 500)]
    txn_batch_size: u64,

    // each rpc sender will send 1 batch of txn_batch_size transactions every rpc_sender_interval_ms
    #[arg(long, default_value_t = 16)]
    num_rpc_senders: u64,

    /// interval at which each RPC sender sends a batch of transaction to RPC (in milliseconds)
    #[arg(long, default_value_t = 1000)]
    rpc_sender_interval_ms: u64,

    #[arg(value_enum, long, default_value_t = TxgenStrategy::UniformManyToMany)]
    txgen_strategy: TxgenStrategy,

    #[arg(long, default_value_t = 2)]
    verbosity: u8,
}

#[derive(ValueEnum, Clone, Copy)]
enum TxgenStrategy {
    UniformManyToMany,
    ManyToOne,
    OneToMany,
    HotCold,
}

impl ToString for TxgenStrategy {
    fn to_string(&self) -> String {
        match self {
            TxgenStrategy::UniformManyToMany => "uniform-many-to-many",
            TxgenStrategy::ManyToOne => "many-to-one",
            TxgenStrategy::OneToMany => "one-to-many",
            TxgenStrategy::HotCold => "hot-cold",
        }
        .into()
    }
}

// ------------------- Transaction generator functions -------------------

/// creates batches of transactions that send between pairs of accounts
/// pairs are chosen according to the passed in selection functions
async fn send_between_rand_pairings(
    accounts: &mut [Account],
    strat: TxgenStrategy,
    value: u128,
    txn_batch_size: usize,
    txn_sender: Sender<Vec<Bytes>>,
    num_batches: usize,
) {
    let mut rng = rand::rngs::SmallRng::from_entropy();
    let idx = rng.gen_range(0..accounts.len());

    for _ in 0..num_batches {
        let mut raw_txns = Vec::new();
        for _ in 0..txn_batch_size {
            let uniform = |len, rng: &mut SmallRng| {
                let first = rng.gen_range(0..len);
                let mut second;
                loop {
                    second = rng.gen_range(0..len);
                    if second != first {
                        return (first, second);
                    }
                }
            };
            let choose_other = |idx: usize, len, rng: &mut SmallRng| {
                let mut other_idx;
                loop {
                    other_idx = rng.gen_range(0..len);
                    if other_idx != idx {
                        return (idx, other_idx);
                    }
                }
            };
            let (src, dst) = match strat {
                TxgenStrategy::UniformManyToMany => uniform(accounts.len(), &mut rng),
                TxgenStrategy::ManyToOne => {
                    let (dst, src) = choose_other(idx, accounts.len(), &mut rng);
                    (src, dst)
                }
                TxgenStrategy::OneToMany => choose_other(idx, accounts.len(), &mut rng),
                TxgenStrategy::HotCold => {
                    if rng.gen_bool(0.2) {
                        uniform(accounts.len(), &mut rng)
                    } else {
                        choose_other(idx, accounts.len(), &mut rng)
                    }
                }
            };
            let dst_addr = accounts[dst].address;
            let src_acct = &mut accounts[src];

            raw_txns.push(src_acct.create_raw_xfer_transaction(dst_addr, value));
        }

        let _ = txn_sender.send(raw_txns).await.unwrap();
    }
}

/// Waits for a batch of refreshed accounts, generates num_txns_batches batches of
/// transactions from those accounts with account selector based off TxgenStrategy.
/// Sends the accounts through the completed channel after the transaction generation
async fn generate_pairings(
    accounts_ready_receiver: Receiver<Vec<Account>>,
    accounts_completed_sender: Sender<Vec<Account>>,
    num_txns_batches: usize,
    txn_batch_size: usize,
    txn_batch_sender: Sender<Vec<Bytes>>,
    strat: TxgenStrategy,
) {
    let mut num_ready_batches = 0;
    loop {
        match accounts_ready_receiver.recv().await {
            Ok(mut accounts_slice) => {
                // received accounts slice to generate transactions with
                num_ready_batches += 1;
                println!(
                    "generate uniform rand pairings: received refreshed accounts. batch {}",
                    num_ready_batches
                );

                let transfer_value = 10;
                send_between_rand_pairings(
                    &mut accounts_slice,
                    strat,
                    transfer_value,
                    txn_batch_size,
                    txn_batch_sender.clone(),
                    num_txns_batches,
                )
                .await;

                let _ = accounts_completed_sender.send(accounts_slice).await;
                println!(
                    "generate uniform rand pairings: sent completed accounts. batch {}",
                    num_ready_batches
                );
            }
            Err(err) => {
                // channel is closed and no more accounts exist
                println!("generate uniform rand pairings channel: {}", err);
                break;
            }
        }
    }
}

// ------------------- Pipeline creation functions -------------------

async fn start_random_tx_gen_unbounded(
    final_accounts: Vec<Account>,
    client: Client,
    txn_batch_size: usize,
    txn_batch_sender: Sender<Vec<Bytes>>,
    strat: TxgenStrategy,
) {
    let (refresh_context_sender, refresh_context_receiver) =
        async_channel::bounded(ACCOUNTS_BATCHES_CHANNEL_BUFFER);
    tokio::spawn(accounts_refresher(
        refresh_context_receiver,
        txn_batch_size,
        client.clone(),
    ));

    let (generate_rand_pairings_sender, generate_rand_pairings_receiver) =
        async_channel::bounded(ACCOUNTS_BATCHES_CHANNEL_BUFFER);
    let (accounts_completed_sender, accounts_completed_receiver) =
        async_channel::bounded(ACCOUNTS_BATCHES_CHANNEL_BUFFER);

    tokio::spawn(generate_pairings(
        generate_rand_pairings_receiver,
        accounts_completed_sender,
        NUM_TXN_BATCHES_PER_ACCOUNTS_BATCH,
        txn_batch_size,
        txn_batch_sender.clone(),
        strat,
    ));

    let accounts_batches =
        split_final_accounts_into_batches(final_accounts, NUM_ACCOUNTS_PER_BATCH);

    // start the pipeline
    let new_refresh_context_sender = refresh_context_sender.clone();
    let new_generate_rand_pairings_sender = generate_rand_pairings_sender.clone();
    tokio::spawn(async move {
        for accounts in accounts_batches {
            new_refresh_context_sender
                .send(AccountsRefreshContext {
                    accounts_to_refresh: accounts,
                    ready_sender: new_generate_rand_pairings_sender.clone(),
                })
                .await
                .unwrap();
        }
    });

    let mut completed_batch_num = 0;
    println!("unbounded tx gen: starting loop");
    // receive a completed batch and reinsert it into the pipeline.
    loop {
        let completed_batch = accounts_completed_receiver
            .recv()
            .await
            .expect("completed batch channel must not close");

        completed_batch_num += 1;
        println!(
            "unbounded tx gen: received completed batch {}",
            completed_batch_num
        );

        refresh_context_sender
            .send(AccountsRefreshContext {
                accounts_to_refresh: completed_batch,
                ready_sender: generate_rand_pairings_sender.clone(),
            })
            .await
            .unwrap();

        println!(
            "unbounded tx gen: re-sent completed batch {}",
            completed_batch_num
        );
    }
}

async fn tps_logging(tx_counter: Arc<AtomicUsize>, tx_receiver: Receiver<Vec<Bytes>>, verbosity: u8) {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    let mut last_tx_count = tx_counter.load(Ordering::SeqCst);
    let mut last_instant = Instant::now();
    if verbosity > 1 {
        println!(
            "tx_receiver channel capacity: {}",
            tx_receiver.capacity().unwrap()
        );
    }

    loop {
        let instant = interval.tick().await;
        let new_tx_count = (&tx_counter).load(Ordering::SeqCst);
        let txns = new_tx_count - last_tx_count;

        let secs = (instant - last_instant).as_secs_f64();
        let tps = txns as f64 / secs;
        println!(
            "Sent {} txns in {} seconds at {} tps, tx_receiver len: {}",
            txns,
            secs,
            tps,
            tx_receiver.len()
        );
        last_tx_count = new_tx_count;
        last_instant = instant;
    }
}

fn spawn_tps_logging(tx_batch_counter: Arc<AtomicUsize>, tx_receiver: Receiver<Vec<Bytes>>, verbosity: u8) {
    tokio::spawn(async move {
        tps_logging(tx_batch_counter, tx_receiver, verbosity).await;
    });
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let start = Instant::now();

    let args = Args::parse();

    let client = Client::new(args.rpc_url);

    let num_rpc_senders = args.num_rpc_senders as usize;
    let txn_batch_size = args.txn_batch_size as usize;

    // let rpc_sender_interval = Duration::from_millis(args.rpc_sender_interval_ms);
    let rpc_sender_interval = Duration::from_millis(args.rpc_sender_interval_ms);
    let expected_tps =
        (args.num_rpc_senders * args.txn_batch_size) as f64 / rpc_sender_interval.as_secs_f64();
    println!(
        "num rpc senders: {}. batch size: {}. expected TPS ~ {}",
        args.num_rpc_senders, args.txn_batch_size, expected_tps
    );

    let (txn_batch_sender, txn_batch_receiver) = async_channel::bounded(TX_BATCHES_CHANNEL_BUFFER);
    let tx_batch_counter = Arc::new(AtomicUsize::new(0));
    if args.verbosity > 0 {
        spawn_tps_logging(tx_batch_counter.clone(), txn_batch_receiver.clone(), args.verbosity);
    }

    let rpc_sender_handles = {
        let mut rpc_sender_handles = Vec::new();
        for id in 0..num_rpc_senders {
            rpc_sender_handles.push(tokio::spawn(rpc_sender(
                id,
                rpc_sender_interval,
                txn_batch_receiver.clone(),
                txn_batch_size,
                client.clone(),
                tx_batch_counter.clone(),
            )))
        }

        rpc_sender_handles
    };

    let root_account = Account::new(args.root_private_key);
    let num_final_accounts = args.num_final_accounts as usize;

    let final_accounts = make_final_accounts(
        root_account,
        num_final_accounts,
        args.load_final_accs,
        args.priv_keys_file,
        client.clone(),
        txn_batch_size,
        txn_batch_sender.clone(),
        args.verbosity,
    )
    .await;
    let num_final_accounts = final_accounts.len();
    println!("{} final accounts created", num_final_accounts);

    if !args.final_accs_only {
        start_random_tx_gen_unbounded(
            final_accounts,
            client.clone(),
            txn_batch_size,
            txn_batch_sender.clone(),
            args.txgen_strategy,
        )
        .await;
    }

    txn_batch_sender.close();
    let _rpc_sender_results = join_all(rpc_sender_handles).await;

    let stop = Instant::now();
    let time = stop.duration_since(start).as_millis();

    println!("ran for {}. num txns: {:?}.", time, tx_batch_counter);

    Ok(())
}
