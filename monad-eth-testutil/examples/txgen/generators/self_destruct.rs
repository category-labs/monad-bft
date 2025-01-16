use crate::{
    prelude::*,
    shared::erc20::{calculate_contract_addr, ERC20},
};

pub struct SelfDestructTxGenerator {
    pub tx_per_sender: usize,
    pub contracts: Vec<ERC20>,
    // TODO: add option to fill up storage to a certain level before marking the contract eligble for destruction
}

impl Generator for SelfDestructTxGenerator {
    fn handle_acct_group(
        &mut self,
        accts: &mut [SimpleAccount],

        ctx: &GenCtx,
    ) -> Vec<(TxEnvelope, Address)> {
        let mut idxs: Vec<usize> = (0..accts.len()).collect();
        let mut rng = SmallRng::from_entropy();
        let mut txs = Vec::with_capacity(self.tx_per_sender * accts.len());

        debug!(contract = self.contracts.len(), "Number of contracts");

        for _ in 0..self.tx_per_sender {
            idxs.shuffle(&mut rng);

            for &idx in &idxs {
                let sender = &mut accts[idx];

                // aim to keep ~1000 contracts
                let contracts_idx = rng.gen_range(0..1000);
                if contracts_idx < self.contracts.len() {
                    let contract = self.contracts.swap_remove(contracts_idx);
                    trace!(
                        addr = contract.addr.to_string(),
                        "Self destructing contract"
                    );

                    txs.push((
                        contract.self_destruct_tx(sender, ctx.base_fee, ctx.chain_id),
                        contract.addr,
                    ))
                } else {
                    let addr = calculate_contract_addr(&sender.addr, sender.nonce);
                    trace!(addr = addr.to_string(), "Deploying contract");

                    self.contracts.push(ERC20 { addr });
                    // TODO: ugly inconsistency
                    let tx =
                        ERC20::deploy_tx(sender.nonce, &sender.key, ctx.base_fee * 2, ctx.chain_id);
                    sender.nonce += 1;
                    txs.push((tx, addr));
                }
            }
        }

        txs
    }
}
