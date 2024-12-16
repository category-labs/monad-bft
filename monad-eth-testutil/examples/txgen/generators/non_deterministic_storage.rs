use crate::{
    prelude::*,
    shared::erc20::{ERC20, IERC20},
};

pub struct NonDeterministicStorageTxGenerator {
    pub(crate) recipient_keys: SeededKeyPool,
    pub(crate) erc20: ERC20,
    pub(crate) tx_per_sender: usize,
}

// note: we need to mint first
impl Generator for NonDeterministicStorageTxGenerator {
    fn handle_acct_group(
        &mut self,
        accts: &mut [SimpleAccount],
        ctx: &GenCtx,
    ) -> Vec<(TxEnvelope, Address)> {
        let mut idxs: Vec<usize> = (0..accts.len()).collect();
        let mut rng = SmallRng::from_entropy();
        let mut txs = Vec::with_capacity(self.tx_per_sender * accts.len());

        for _ in 0..(self.tx_per_sender) {
            idxs.shuffle(&mut rng);

            for &idx in &idxs {
                let from = &mut accts[idx];

                if rng.gen_bool(0.3) {
                    txs.push((
                        self.erc20.construct_tx(
                            from,
                            IERC20::transferToFriendsCall {
                                amount: U256::from(10),
                            },
                            ctx.base_fee,
                        ),
                        from.addr,
                    ));
                } else {
                    let to = self.recipient_keys.next_addr(); // change sampling strategy?
                    txs.push((
                        self.erc20.construct_tx(
                            from,
                            IERC20::addFriendCall { friend: to },
                            ctx.base_fee,
                        ),
                        to,
                    ));
                };
            }
        }

        txs
    }
}
