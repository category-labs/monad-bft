use crate::{prelude::*, shared::erc20::ERC20};

pub struct HighCallDataTxGenerator {
    pub(crate) recipient_keys: SeededKeyPool,
    pub(crate) tx_per_sender: usize,
}

impl Generator for HighCallDataTxGenerator {
    fn handle_acct_group(
        &mut self,
        accts: &mut [SimpleAccount],
        ctx: &GenCtx,
    ) -> Vec<(TransactionSigned, Address)> {
        let mut txs = Vec::with_capacity(accts.len());

        for sender in accts {
            for _ in 0..self.tx_per_sender {
                let to = self.recipient_keys.next_addr();

                let tx = ERC20::deploy_tx(sender.nonce, &sender.key, ctx.base_fee * 2);
                sender.nonce += 1;

                txs.push((tx, to));
            }
        }

        txs
    }
}
