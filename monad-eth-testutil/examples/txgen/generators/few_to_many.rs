use super::{erc20_transfer, native_transfer};
use crate::{prelude::*, shared::erc20::ERC20, TxType};

pub struct CreateAccountsGenerator {
    pub recipient_keys: SeededKeyPool,
    pub tx_per_sender: usize,
    pub tx_type: TxType,
    pub erc20: ERC20,
}

impl Generator for CreateAccountsGenerator {
    fn handle_acct_group(
        &mut self,
        accts: &mut [SimpleAccount],
    ) -> Vec<(TransactionSigned, Address)> {
        let mut txs = Vec::with_capacity(accts.len());

        for sender in accts {
            for _ in 0..self.tx_per_sender {
                let to = self.recipient_keys.next_addr();

                let tx = match self.tx_type {
                    TxType::ERC20 => erc20_transfer(sender, to, U256::from(10), &self.erc20),
                    TxType::Native => native_transfer(sender, to, U256::from(10)),
                };

                txs.push((tx, to));
            }
        }

        txs
    }
}
