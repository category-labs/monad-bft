use crate::{
    prelude::*,
    shared::erc20::{ERC20, IERC20},
};

pub struct StorageDeletesTxGenerator {
    pub recipient_keys: SeededKeyPool,
    pub erc20: ERC20,
    pub tx_per_sender: usize,
}

// note: we need to mint first
impl Generator for StorageDeletesTxGenerator {
    fn handle_acct_group(
        &mut self,
        accts: &mut [SimpleAccount],
    ) -> Vec<(TransactionSigned, Address)> {
        let mut idxs: Vec<usize> = (0..accts.len()).collect();
        let mut rng = SmallRng::from_entropy();
        let mut txs = Vec::with_capacity(self.tx_per_sender * accts.len());

        for _ in 0..(self.tx_per_sender) {
            idxs.shuffle(&mut rng);

            for &idx in &idxs {
                let from = &mut accts[idx];
                let to = self.recipient_keys.next_addr();

                let tx = if rng.gen_bool(0.3) {
                    self.erc20
                        .construct_tx(from, IERC20::resetCall { addr: to })
                } else {
                    self.erc20.construct_tx(
                        from,
                        IERC20::transferCall {
                            recipient: to,
                            amount: U256::from(10),
                        },
                    )
                };

                txs.push((tx, to));
            }
        }

        txs
    }
}
