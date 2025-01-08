use alloy_primitives::hex;

use super::*;
use crate::shared::ecmul::ECMul;

pub struct ECMulGenerator {
    pub ecmul: ECMul,
    pub tx_per_sender: usize,
}

impl Generator for ECMulGenerator {
    fn handle_acct_group(
        &mut self,
        accts: &mut [SimpleAccount],
        ctx: &GenCtx,
    ) -> Vec<(TxEnvelope, Address)> {
        let mut txs = Vec::with_capacity(self.tx_per_sender * accts.len());

        for sender in accts {
            for _ in 0..self.tx_per_sender {
                let tx = self.ecmul.construct_tx(sender, ctx.base_fee * 2);
                println!("tx: 0x{}", hex::encode(alloy_rlp::encode(&tx)));
                txs.push((tx, self.ecmul.addr));
            }
        }

        txs
    }
}
