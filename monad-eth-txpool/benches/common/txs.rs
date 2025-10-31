// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use alloy_consensus::{transaction::Recovered, TxEnvelope};
use alloy_primitives::{Uint, B256};
use itertools::Itertools;
use monad_eth_testutil::{make_legacy_tx, recover_tx};
use monad_tfm::base_fee::MIN_BASE_FEE;
use rand::{seq::SliceRandom, Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

const TRANSACTION_SIZE_BYTES: usize = 400;

pub struct BenchTxSetup {
    pub accounts: usize,
    pub txs: usize,
    pub nonce_var: usize,
    pub pending_blocks: usize,
}

pub fn generate_txs(
    accounts: usize,
    txs: usize,
    nonce_var: usize,
    pending_blocks: usize,
) -> (Vec<Vec<Recovered<TxEnvelope>>>, Vec<Recovered<TxEnvelope>>) {
    let mut rng = ChaCha8Rng::seed_from_u64(0);

    let mut accounts = (0..accounts)
        .map(|_| (B256::from(Uint::from(rng.gen::<u64>())), 0u64))
        .collect_vec();

    let pending_block_txs = (0..pending_blocks)
        .map(|pending_block| {
            let mut txs = (txs * pending_block..txs * (pending_block + 1))
                .map(|idx| {
                    let accounts_len = accounts.len();

                    let (account, nonce) = accounts
                        .get_mut(idx % accounts_len)
                        .expect("account idx is in range");

                    let tx = make_legacy_tx(
                        *account,
                        rng.gen_range(MIN_BASE_FEE..=MIN_BASE_FEE + 10000).into(),
                        30000,
                        *nonce,
                        TRANSACTION_SIZE_BYTES,
                    );

                    *nonce += 1;

                    recover_tx(tx)
                })
                .collect_vec();

            txs.shuffle(&mut rng);

            txs
        })
        .collect_vec();

    let mut txs = (0..txs)
        .map(|idx| {
            let (account, nonce) = accounts
                .get(idx % accounts.len())
                .expect("account idx is in range");

            let tx = make_legacy_tx(
                *account,
                rng.gen_range(MIN_BASE_FEE..=MIN_BASE_FEE + 10000).into(),
                30000,
                nonce
                    .checked_add(rng.gen_range(0..=nonce_var as u64))
                    .expect("nonce does not overflow"),
                TRANSACTION_SIZE_BYTES,
            );

            recover_tx(tx)
        })
        .collect_vec();

    txs.shuffle(&mut rng);

    (pending_block_txs, txs)
}
