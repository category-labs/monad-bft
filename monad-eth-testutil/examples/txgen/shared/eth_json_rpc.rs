use std::time::Duration;

use alloy_consensus::TxEnvelope;
use alloy_primitives::{Address, Bytes, U256, U64};
use alloy_rpc_client::ReqwestClient;
use eyre::Result;
use monad_types::DropTimer;
use tracing::trace;

pub trait EthJsonRpc {
    async fn send_raw_transaction_params(&self, tx: TxEnvelope) -> (&'static str, Bytes);
    async fn get_transaction_count(&self, addr: &Address) -> Result<u64>;
    async fn get_balance(&self, addr: &Address) -> Result<U256>;
    // async fn get_erc20_balance(&self, addr: &Address, erc20: ERC20) -> Result<U256>;
    async fn get_code(&self, addr: &Address) -> Result<String>;

    async fn batch_get_balance(
        &self,
        addrs: impl std::iter::ExactSizeIterator<Item = &Address>,
    ) -> Result<Vec<Result<(Address, U256)>>>;
    async fn batch_get_transaction_count(
        &self,
        addrs: impl std::iter::ExactSizeIterator<Item = &Address>,
    ) -> Result<Vec<Result<(Address, u64)>>>;
    // async fn batch_get_erc20_balance(
    //     &self,
    //     addrs: impl std::iter::ExactSizeIterator<Item = &Address>,
    //     erc20: ERC20,
    // ) -> Result<Vec<Result<(Address, U256)>>>;
}

impl EthJsonRpc for ReqwestClient {
    async fn get_transaction_count(&self, addr: &Address) -> Result<u64> {
        let addr = addr.to_string();
        let nonce = self
            .request::<_, U64>("eth_getTransactionCount", [&addr, "latest"])
            .await?;
        Ok(nonce.to())
    }

    async fn send_raw_transaction_params(&self, tx: TxEnvelope) -> (&'static str, Bytes) {
        ("eth_sendRawTransaction", alloy_rlp::encode(tx).into())
    }

    async fn get_code(&self, addr: &Address) -> Result<String> {
        let addr = addr.to_string();
        self.request::<_, _>("eth_getCode", [&addr, "latest"])
            .await
            .map_err(Into::into)
    }

    async fn get_balance(&self, addr: &Address) -> Result<U256> {
        let addr = addr.to_string();
        self.request::<_, U256>("eth_getBalance", [&addr, "latest"])
            .await
            .map_err(Into::into)
    }

    // async fn get_erc20_balance(&self, addr: &Address, erc20: ERC20) -> Result<U256> {
    //     let (method, params) = erc20.balance_of(*addr);
    //     self.request::<_, U256>(method, params)
    //         .await
    //         .map_err(Into::into)
    // }

    async fn batch_get_balance(
        &self,
        addrs: impl std::iter::ExactSizeIterator<Item = &Address>,
    ) -> Result<Vec<Result<(Address, U256)>>> {
        let _drop_timer = log_elapsed(&addrs, "batch_get_balance");
        let mut output = Vec::with_capacity(addrs.len());
        let mut batch = self.new_batch();

        let futs: Vec<_> = addrs
            .map(|addr| {
                let params = [&addr.to_string(), "latest"];
                batch
                    .add_call::<_, U256>("eth_getBalance", &params)
                    .map(|r| async move { r.await.map(|b| (*addr, b)) })
            })
            .collect::<Result<Vec<_>, _>>()?;

        batch.send().await?;

        for fut in futs {
            output.push(fut.await.map_err(Into::into));
        }

        Ok(output)
    }

    async fn batch_get_transaction_count(
        &self,
        addrs: impl std::iter::ExactSizeIterator<Item = &Address>,
    ) -> Result<Vec<Result<(Address, u64)>>> {
        let _drop_timer = log_elapsed(&addrs, "batch_get_transaction_count");
        let mut output = Vec::with_capacity(addrs.len());
        let mut batch = self.new_batch();

        let futs: Vec<_> = addrs
            .map(|addr| {
                let params = [&addr.to_string(), "latest"];
                batch
                    .add_call::<_, U64>("eth_getTransactionCount", &params)
                    .map(|r| async move { r.await.map(|b| (*addr, b)) })
            })
            .collect::<Result<Vec<_>, _>>()?;

        batch.send().await?;

        for fut in futs {
            output.push(fut.await.map(|(a, n)| (a, n.to())).map_err(Into::into));
        }

        Ok(output)
    }

    // async fn batch_get_erc20_balance(
    //     &self,
    //     addrs: impl std::iter::ExactSizeIterator<Item = &Address>,
    //     erc20: ERC20,
    // ) -> Result<Vec<Result<(Address, U256)>>> {
    //     let _drop_timer = log_elapsed(&addrs, "batch_get_erc20_balance");
    //     let mut output = Vec::with_capacity(addrs.len());
    //     let mut batch = self.new_batch();

    //     let futs: Vec<_> = addrs
    //         .map(|addr| {
    //             let (method, params) = erc20.balance_of(*addr);
    //             batch
    //                 .add_call::<_, U256>(method, &params)
    //                 .map(|r| async move { r.await.map(|b| (*addr, b)) })
    //         })
    //         .collect::<Result<Vec<_>, _>>()?;

    //     batch.send().await?;

    //     for fut in futs {
    //         output.push(fut.await.map_err(Into::into));
    //     }

    //     Ok(output)
    // }
}

fn log_elapsed<T>(
    a: &impl ExactSizeIterator<Item = T>,
    msg: &'static str,
) -> DropTimer<impl Fn(Duration)> {
    let num_addrs = a.len();
    DropTimer::start(Duration::from_millis(5), move |elapsed: Duration| {
        trace!(num_addrs, elapsed_ms = elapsed.as_millis(), msg);
    })
}
