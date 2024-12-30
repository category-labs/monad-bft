use super::{erc20_transfer, native_transfer};
use crate::{prelude::*, shared::erc20::ERC20, TxType};

pub struct ManyToManyGenerator {
    pub recipient_keys: SeededKeyPool,
    pub tx_per_sender: usize,
    pub tx_type: TxType,
    pub erc20: ERC20,
}

impl Generator for ManyToManyGenerator {
    fn handle_acct_group(
        &mut self,
        accts: &mut [SimpleAccount],
        ctx: &GenCtx,
    ) -> Vec<(TransactionSigned, Address)> {
        let mut idxs: Vec<usize> = (0..accts.len()).collect();
        let mut rng = SmallRng::from_entropy();
        let mut txs = Vec::with_capacity(self.tx_per_sender * accts.len());

        for _ in 0..self.tx_per_sender {
            idxs.shuffle(&mut rng);

            for &idx in &idxs {
                let sender = &mut accts[idx];
                let to = self.recipient_keys.next_addr(); // change sampling strategy?

                let tx = match self.tx_type {
                    TxType::ERC20 => erc20_transfer(sender, to, U256::from(10), &self.erc20, ctx),
                    TxType::Native => native_transfer(sender, to, U256::from(10), ctx),
                };

                txs.push((tx, to));
            }
        }

        txs
    }
}
