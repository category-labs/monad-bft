use std::{
    fs::File,
    future::Future,
    io::{BufRead, BufReader, BufWriter, Write},
};

use async_channel::Sender;
use futures::{future::join_all, StreamExt};
use reth_primitives::Bytes;
use tokio::time::{sleep, Instant};

use crate::{batch_refresh_accounts, Account, Client, EXECUTION_DELAY_WAIT_TIME, TXN_GAS_FEES};

// ------------------- Final accounts functions -------------------

async fn split_account_balance(
    mut account_to_split: Account,
    num_new_accounts: usize,
    txn_batch_size: usize,
    txn_sender: Sender<Vec<Bytes>>,
) -> Vec<Account> {
    if account_to_split.balance == 0 {
        // If balance is expected to be non-zero, this error could mean either:
        // 1. The transfer transaction sent to this account didn't make it through consensus, or
        // 2. Execution hasn't run far enough to reflect the accurate balance for this account

        println!(
            "account {} has zero balance, expected > 0",
            account_to_split.address
        );
        // FIXME: should wait for balance instead of returning empty ?
        return Vec::new();
    }

    let num_splits = (num_new_accounts + 1) as u128; // leave some tokens in the root account
    let transfer_amount = account_to_split
        .balance
        // .saturating_sub(MAX_RESERVE_BALANCE_PER_ACCOUNT as u128)
        .saturating_sub(TXN_GAS_FEES as u128 * num_splits);
    if transfer_amount < num_splits {
        println!(
            "account {} doesn't have enough execution balance for splitting. balance: {}",
            account_to_split.address, transfer_amount
        );
        return Vec::new();
    }
    let transfer_per_account = transfer_amount.saturating_div(num_splits);

    let mut new_accounts = create_rand_accounts(num_new_accounts);
    for dst_accounts in new_accounts.chunks_mut(txn_batch_size) {
        let txns_batch =
            create_transfer_txns(&mut account_to_split, dst_accounts, transfer_per_account);
        let _ = txn_sender.send(txns_batch).await.unwrap();
    }

    new_accounts
}

async fn create_final_accounts_from_root(
    root_account: Account,
    mut num_final_accounts: usize,
    client: Client,
    txn_batch_size: usize,
    txn_sender: Sender<Vec<Bytes>>,
    verbosity: u8,
) -> Vec<Account> {
    // Calculates the number of splits based on BATCH_SIZE.
    // e.g. For 1,000,000 final accounts and BATCH_SIZE = 500, root accounts will be split into 4
    // accounts, followed by 500 per account, followed by 500 per account.
    // Total accounts created = 4 * 500 * 500 = 1,000,000
    let mut split_levels = Vec::new();
    while num_final_accounts > txn_batch_size {
        split_levels.push(txn_batch_size);
        // This may create more accounts than num_final_accounts
        num_final_accounts = num_final_accounts.div_ceil(txn_batch_size);
    }
    split_levels.push(num_final_accounts);

    let mut accounts_to_split = vec![root_account];
    for (split_level, num_new_accounts_per_split) in split_levels.iter().rev().enumerate() {
        // This sleep is to wait for execution to catch up so that
        // nonce and balance are up to date when refreshed before splitting further
        sleep(EXECUTION_DELAY_WAIT_TIME).await;

        let expected_num_new_accounts = accounts_to_split.len() * num_new_accounts_per_split;
        println!(
            "splitting {} account(s) to {} accounts. level {}",
            accounts_to_split.len(),
            expected_num_new_accounts,
            split_level + 1
        );

        let new_accounts: Vec<Account> = futures::stream::iter(accounts_to_split.into_iter())
            .map(|mut account| {
                let client = client.clone();
                async move {
                    account.refresh_balance(client.clone()).await;
                    account.refresh_nonce(client.clone()).await;
                    account
                }
            })
            .buffer_unordered(10)
            .then(|account| {
                split_account_balance(
                    account,
                    *num_new_accounts_per_split,
                    txn_batch_size,
                    txn_sender.clone(),
                )
            })
            .fold(Vec::new(), |mut accumulator, vec| async move {
                accumulator.extend_from_slice(&vec);
                accumulator
            })
            .await;

        // keep splitting new accounts
        accounts_to_split = new_accounts;
    }

    accounts_to_split
}

/// Splits the final accounts into chunks
/// 10 batches 1,000,000 accounts would return a vec of 100,000 accounts per batch
pub fn split_final_accounts_into_batches(
    mut final_accounts: Vec<Account>,
    num_accounts_per_batch: usize,
) -> Vec<Vec<Account>> {
    let num_final_accounts = final_accounts.len();

    let mut accounts_batches = Vec::new();
    let num_batches = num_final_accounts / num_accounts_per_batch;
    for i in 1..num_batches {
        accounts_batches
            .push(final_accounts.split_off(num_final_accounts - (i * num_accounts_per_batch)))
    }
    accounts_batches.push(final_accounts);

    accounts_batches
}

/// create a list of random accounts
fn create_rand_accounts(num_accounts: usize) -> Vec<Account> {
    (0..num_accounts)
        .into_iter()
        .map(|_| Account::new_random())
        .collect()
}

fn create_transfer_txns(
    root_account: &mut Account,
    dst_accounts: &mut [Account],
    transfer_amount: u128,
) -> Vec<Bytes> {
    let raw_txns = dst_accounts
        .into_iter()
        .map(|dst| root_account.create_raw_xfer_transaction(dst.address, transfer_amount))
        .collect::<Vec<_>>();

    raw_txns
}

pub async fn make_final_accounts(
    root_account: Account,
    num_final_accounts: usize,
    load_final_accs: bool,
    priv_keys_file: Option<String>,
    client: Client,
    txn_batch_size: usize,
    txn_batch_sender: Sender<Vec<Bytes>>,
    verbosity: u8,
) -> Vec<Account> {
    if load_final_accs {
        let Some(priv_keys_file_path) = priv_keys_file else {
            panic!("private keys file not provided");
        };
        println!("loading private keys from {}", priv_keys_file_path);

        let mut final_accounts = Vec::new();

        let file =
            std::fs::File::open(priv_keys_file_path).expect("private keys file should exist");
        let reader = BufReader::new(file);

        for line in reader.lines() {
            let pk = line.expect("each line should be a private key");
            final_accounts.push(Account::new(pk));
        }

        final_accounts
    } else {
        let final_accounts = create_final_accounts_from_root(
            root_account,
            num_final_accounts,
            client.clone(),
            txn_batch_size,
            txn_batch_sender.clone(),
            verbosity,
        )
        .await;

        if let Some(priv_keys_file_path) = priv_keys_file {
            let mut file = BufWriter::new(File::create(priv_keys_file_path).expect("no error"));
            for account in final_accounts.iter() {
                file.write(account.priv_key.to_string().as_bytes()).unwrap();
                file.write(b"\n").unwrap();
            }
            file.flush().unwrap();
        };

        final_accounts
    }
}
