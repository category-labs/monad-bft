use super::*;
use crate::shared::uniswap::Uniswap;

pub struct UniswapGenerator {
    pub uniswap: Uniswap,
    pub tx_per_sender: usize,
}

impl Generator for UniswapGenerator {
    fn handle_acct_group(
        &mut self,
        accts: &mut [SimpleAccount],
        ctx: &GenCtx,
    ) -> Vec<(TxEnvelope, Address)> {
        let mut txs = Vec::with_capacity(self.tx_per_sender * accts.len());

        // for each sender, provide liquidity in uniswap pools
        for sender in accts {
            for _ in 0..self.tx_per_sender {
                let tx = self.uniswap.construct_tx(sender, ctx.base_fee);
                txs.push((tx, self.uniswap.addr));
            }
        }

        txs
    }
}
