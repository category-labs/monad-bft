
use async_channel::{Receiver, Sender};
use serde_json::{json, Value};
use tokio::{
    join, time::Instant
};

use crate::{Account, Client, JsonResponse, EXECUTION_DELAY_WAIT_TIME};

// ------------------- Refresher functions -------------------

pub async fn batch_refresh_accounts(accounts: &mut [Account], client: Client, txn_batch_size: usize) {
    assert!(accounts.len() == txn_batch_size);

    // TODO: implement eth_getAccount in RPC and make this a sinlge call ?
    // create BATCH_SIZE requests for nonce updates
    let json_values = accounts
        .iter()
        .enumerate()
        .map(|(id, acc)| {
            json!({
                    "jsonrpc": "2.0",
                    "method": "eth_getTransactionCount",
                    "params": [acc.address, "latest"],
                    "id": id,
            })
        })
        .collect::<Vec<Value>>();
    let batch_req = serde_json::Value::Array(json_values);
    let nonces_res_future = client.rpc(batch_req);

    // create BATCH_SIZE requests for balance updates
    let json_values = accounts
        .iter()
        .enumerate()
        .map(|(id, acc)| {
            json!({
                    "jsonrpc": "2.0",
                    "method": "eth_getBalance",
                    "params": [acc.address, "latest"],
                    "id": id,
            })
        })
        .collect::<Vec<Value>>();
    let batch_req = serde_json::Value::Array(json_values);
    let balances_res_future = client.rpc(batch_req);
    let (nonces_res, balances_res) = join!(nonces_res_future, balances_res_future);

    let latest_nonces = match nonces_res {
        Ok(nonces_res) => match nonces_res.json::<Vec<JsonResponse>>().await {
            Ok(latest_nonces) => latest_nonces,
            Err(err) => {
                println!("nonces refresh response, json deser error: {}", err);
                return;
            }
        },
        Err(err) => {
            println!("rpc not responding for nonces refresh: {}", err);
            return;
        }
    };

    let latest_balances = match balances_res {
        Ok(balances_res) => match balances_res.json::<Vec<JsonResponse>>().await {
            Ok(latest_balances) => latest_balances,
            Err(err) => {
                println!("balances refresh response, json deser error: {}", err);
                return;
            }
        },
        Err(err) => {
            println!("rpc not responding for balances refresh: {}", err);
            return;
        }
    };

    for single_res in latest_nonces {
        accounts[single_res.id].nonce = single_res.get_result_u128() as u64;
    }
    for single_res in latest_balances {
        accounts[single_res.id].balance = single_res.get_result_u128();
    }
}

pub struct AccountsRefreshContext {
    pub accounts_to_refresh: Vec<Account>,
    pub ready_sender: Sender<Vec<Account>>,
}

/// Waits for a AccountsRefreshContext, updates the accounts' nonces and balances,
/// and sends the refreshed accounts through the channel in the AccountsRefreshContext
pub async fn accounts_refresher(
    refresh_context_receiver: Receiver<AccountsRefreshContext>,
    txn_batch_size: usize,
    client: Client,
) {
    println!("starting account refresher");
    let mut num_refreshes = 0;
    loop {
        // receive a refresh context
        // if err, tx gen has completed and no more accounts to refresh. end loop
        match refresh_context_receiver.recv().await {
            Ok(mut refresh_context) => {
                // received some accounts to refresh

                num_refreshes += 1;
                println!(
                    "accounts refresher: received accounts to refresh. batch {}",
                    num_refreshes
                );
                let start = Instant::now();

                // wait for execution delay before refreshing
                tokio::time::sleep(EXECUTION_DELAY_WAIT_TIME).await;

                // refresh nonce and balance of every account
                for accounts in refresh_context
                    .accounts_to_refresh
                    .chunks_mut(txn_batch_size)
                {
                    batch_refresh_accounts(accounts, client.clone(), txn_batch_size).await;
                }

                // send the refreshed accounts through the ready sender
                refresh_context
                    .ready_sender
                    .send(refresh_context.accounts_to_refresh)
                    .await
                    .expect("ready sender error");

                let time_taken_ms = Instant::now().duration_since(start).as_millis();
                println!(
                    "accounts refresher: sent refreshed accounts. batch {}. took {}ms",
                    num_refreshes, time_taken_ms
                );
            }
            Err(err) => {
                println!("accounts refresher receiver channel: {}", err);
                break;
            }
        }
    }
    println!("stopped account refresher");
}
