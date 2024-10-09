use std::{future::Future, sync::Arc, time::Duration};

use alloy_json_rpc::RpcError;
use alloy_primitives::FixedBytes;
use alloy_rpc_client::ReqwestClient;
use alloy_transport::TransportErrorKind;
use futures::{FutureExt, StreamExt};
use reth_primitives::U256;
use reth_rpc_types::{Block, BlockTransactions};
use ruint::Uint;
use thiserror::Error;
use tokio::{sync::RwLock, task::JoinHandle, time::interval};

use super::{
    blockstream::{BlockStream, BlockStreamError},
    ChainAccountState, ChainState, ChainStateView, SharedChainState,
};

pub struct ChainStateManager {
    client: ReqwestClient,
    blockstream: BlockStream,
    chain_state: SharedChainState,
}

impl ChainStateManager {
    pub async fn new(client: ReqwestClient) -> (Self, ChainStateView) {
        let blockstream = BlockStream::new(client.clone(), Duration::from_millis(25)).await;

        let chain_state = Arc::new(RwLock::new(ChainState::default()));

        (
            Self {
                client,
                blockstream,
                chain_state: chain_state.clone(),
            },
            ChainStateView::new(chain_state),
        )
    }

    pub fn start(self) -> ChainStateManagerHandle {
        ChainStateManagerHandle(tokio::spawn(self.run()))
    }

    async fn run(mut self) -> Result<(), ChainStateManagerError> {
        let mut fetch_new_accounts_timer = interval(Duration::from_millis(500));

        loop {
            tokio::select! {
                biased;

                result = self.blockstream.select_next_some() => {
                    let () = self.process_new_block(result?).await?;
                }
                _ = fetch_new_accounts_timer.tick() => {
                    let () = self.fetch_new_accounts().await?;
                }
            }
        }
    }

    async fn process_new_block(&mut self, block: Block) -> Result<(), ChainStateManagerError> {
        let transactions = match block.transactions {
            BlockTransactions::Full(transactions) => transactions,
            BlockTransactions::Hashes(_) | BlockTransactions::Uncle => {
                if block.header.nonce != Some(FixedBytes::new([0u8; 8])) {
                    panic!("got uncle for non-genesis block");
                }

                return Ok(());
            }
        };

        let mut chain_state = self.chain_state.write().await;

        println!("got block with {} transactions", transactions.len());

        for transaction in transactions {
            if let Some(from_account_state) = chain_state.accounts.get_mut(&transaction.from) {
                let [nonce] = transaction.nonce.into_limbs();

                // TODO(abenedito): Verify balance computation
                let gas_cost = transaction
                    .gas
                    .checked_mul(Uint::<256, 4>::from_limbs_slice(
                        &transaction
                            .gas_price
                            .expect("transaction has gas price")
                            .into_limbs(),
                    ))
                    .expect("gas cost does not overflow");

                from_account_state.balance = from_account_state
                    .balance
                    .checked_sub(transaction.value)
                    .unwrap_or_else(|| {
                        panic!(
                            "balance underflows value for account {:?}",
                            transaction.from
                        )
                    })
                    .checked_sub(gas_cost)
                    .expect("balance does not underflow from gas cost");

                from_account_state.nonce = nonce.checked_add(1).expect("nonce does not overflow");
            }

            if let Some(to_account_state) = transaction
                .to
                .and_then(|to| chain_state.accounts.get_mut(&to))
            {
                to_account_state.balance = to_account_state
                    .balance
                    .checked_add(transaction.value)
                    .expect("balanced does not overflow")
            }
        }

        Ok(())
    }

    async fn fetch_new_accounts(&mut self) -> Result<(), ChainStateManagerError> {
        let Some(current_block_number) = self.blockstream.get_current_block_number() else {
            return Ok(());
        };

        let new_accounts = {
            let chain_state = self.chain_state.read().await;

            chain_state
                .new_accounts
                .iter()
                .take(256)
                .cloned()
                .collect::<Vec<_>>()
        };

        if new_accounts.is_empty() {
            return Ok(());
        }

        let mut batch = self.client.new_batch();

        let calls = new_accounts
            .into_iter()
            .map(|address| {
                let params = [address.to_string(), format!("0x{current_block_number:x}")];

                let balance_fut = batch.add_call("eth_getBalance", &params)?;
                let nonce_fut = batch.add_call("eth_getTransactionCount", &params)?;

                Ok((address, balance_fut, nonce_fut))
            })
            .collect::<Result<Vec<_>, ChainStateManagerError>>()?;

        batch.send().await.expect("batch succeeds");

        let mut chain_state = self.chain_state.write().await;

        for (address, balance_waiter, nonce_waiter) in calls {
            let balance_str: String = balance_waiter.await?;
            let balance = U256::from_str_radix(
                balance_str
                    .strip_prefix("0x")
                    .expect("balance string always has 0x prefix"),
                16,
            )
            .expect("balance string is valid U256");

            let nonce_str: String = nonce_waiter.await?;
            let nonce = u64::from_str_radix(
                nonce_str
                    .strip_prefix("0x")
                    .expect("nonce string always has 0x prefix"),
                16,
            )
            .expect("nonce string is valid u64");

            if !chain_state.new_accounts.remove(&address) {
                panic!("synced new account {address:?} when not requested!");
            }

            if let Some(existing) = chain_state
                .accounts
                .insert(address, ChainAccountState::new(balance, nonce))
            {
                panic!("added address {address:?} when already existing: {existing:#?}");
            }
        }

        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum ChainStateManagerError {
    #[error("TaskTerminated")]
    TaskTerminated,

    #[error(transparent)]
    RpcError(#[from] RpcError<TransportErrorKind>),

    #[error(transparent)]
    BlockStreamError(#[from] BlockStreamError),
}

pub struct ChainStateManagerHandle(JoinHandle<Result<(), ChainStateManagerError>>);

impl Future for ChainStateManagerHandle {
    type Output = ChainStateManagerError;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.0.poll_unpin(cx) {
            std::task::Poll::Ready(Ok(Err(error))) => std::task::Poll::Ready(error),
            std::task::Poll::Ready(Ok(Ok(()))) | std::task::Poll::Ready(Err(_)) => {
                std::task::Poll::Ready(ChainStateManagerError::TaskTerminated)
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}
