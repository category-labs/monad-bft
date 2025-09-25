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

use std::{ops::Deref, time::Duration};

use alloy_primitives::Address;
use indexmap::IndexMap;
use tracing::{error, info};

use super::list::TrackedTxList;
use crate::pool::transaction::ValidEthTransaction;

// To produce 5k tx blocks, we need the tracked tx map to hold at least 15k addresses so that, after
// pruning the txpool of up to 5k unique addresses in the last committed block update and up to 5k
// unique addresses in the pending blocktree, the tracked tx map will still have at least 5k other
// addresses with at least one tx each to use when creating the next block.
const MAX_ADDRESSES: usize = 16 * 1024;

const MAX_TXS: usize = 64 * 1024;

#[derive(Clone, Debug)]
pub(super) struct TrackedTxLimitsConfig {
    max_addresses: usize,
    max_txs: usize,

    soft_tx_expiry: Duration,
    hard_tx_expiry: Duration,
}

impl TrackedTxLimitsConfig {
    pub fn new(soft_tx_expiry: Duration, hard_tx_expiry: Duration) -> Self {
        Self {
            max_addresses: MAX_ADDRESSES,
            max_txs: MAX_TXS,

            soft_tx_expiry,
            hard_tx_expiry,
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct TrackedTxLimits {
    config: TrackedTxLimitsConfig,

    addresses: usize,
    txs: usize,
}

impl TrackedTxLimits {
    pub fn new(config: TrackedTxLimitsConfig) -> Self {
        Self {
            config,

            addresses: 0,
            txs: 0,
        }
    }

    pub fn build_txs_map_with_capacity(&self) -> IndexMap<Address, TrackedTxList> {
        IndexMap::with_capacity(self.config.max_addresses)
    }

    pub fn tx_expiry_during_insert(&self) -> Duration {
        self.config.hard_tx_expiry
    }

    pub fn tx_expiry_during_evict(&self) -> Duration {
        if self.addresses + 512 < self.config.max_addresses {
            self.config.hard_tx_expiry
        } else {
            info!(num_txs =? self.txs, "txpool hit soft evict addresses watermark");

            self.config.soft_tx_expiry
        }
    }

    #[inline]
    fn can_increase_limits(&self, inc_addresses: usize, inc_txs: usize) -> bool {
        let Self {
            config,

            addresses,
            txs,
        } = self;

        if addresses.saturating_add(inc_addresses) > config.max_addresses {
            return false;
        }

        if txs.saturating_add(inc_txs) > config.max_txs {
            return false;
        }

        true
    }

    pub fn prepare_add_tx_to_new_address<'l>(
        &'l mut self,
        tx: ValidEthTransaction,
    ) -> Result<TrackedTxLimitAddToken<'l>, ValidEthTransaction> {
        if !self.can_increase_limits(1, 1) {
            return Err(tx);
        }

        Ok(TrackedTxLimitAddToken {
            kind: TrackedTxLimitAddTokenKind::NewAddress,
            limits: self,
            tx,
        })
    }

    pub fn prepare_add_tx_to_existing<'l>(
        &'l mut self,
        tx: ValidEthTransaction,
    ) -> Result<TrackedTxLimitAddToken<'l>, ValidEthTransaction> {
        if !self.can_increase_limits(0, 1) {
            return Err(tx);
        }

        Ok(TrackedTxLimitAddToken {
            kind: TrackedTxLimitAddTokenKind::Existing,
            limits: self,
            tx,
        })
    }

    pub fn remove_tx(&mut self) {
        let Self {
            config: _,

            addresses: _,
            txs,
        } = self;

        *txs = txs.checked_sub(1).unwrap_or_else(|| {
            error!("txpool txs limit underflowed");
            0
        });
    }

    pub fn remove_address(&mut self) {
        let Self {
            config: _,

            addresses,
            txs: _,
        } = self;

        *addresses = addresses.checked_sub(1).unwrap_or_else(|| {
            error!("txpool address limit underflowed");
            0
        });
    }

    pub fn remove_txs(&mut self, removed_address: bool, removed_txs: usize) {
        let Self {
            config: _,

            addresses,
            txs,
        } = self;

        if removed_address {
            *addresses = addresses.checked_sub(1).unwrap_or_else(|| {
                error!("txpool address limit underflowed");
                0
            });
        }

        *txs = txs.checked_sub(removed_txs).unwrap_or_else(|| {
            error!("txpool txs limit underflowed");
            0
        });
    }

    pub fn reset(&mut self) {
        let Self {
            config: _,

            addresses,
            txs,
        } = self;

        *addresses = 0;
        *txs = 0;
    }
}

pub(super) struct TrackedTxLimitAddToken<'l> {
    kind: TrackedTxLimitAddTokenKind,
    limits: &'l mut TrackedTxLimits,
    tx: ValidEthTransaction,
}

enum TrackedTxLimitAddTokenKind {
    NewAddress,
    Existing,
}

impl<'l> TrackedTxLimitAddToken<'l> {
    pub fn limits(&'l self) -> &'l TrackedTxLimits {
        self.limits
    }

    pub fn finalize(self) -> ValidEthTransaction {
        let Self {
            kind,
            limits:
                TrackedTxLimits {
                    config: _,

                    addresses,
                    txs,
                },
            tx,
        } = self;

        *txs += 1;

        match kind {
            TrackedTxLimitAddTokenKind::NewAddress => {
                *addresses += 1;
            }
            TrackedTxLimitAddTokenKind::Existing => {}
        }

        tx
    }
}

impl<'l> Deref for TrackedTxLimitAddToken<'l> {
    type Target = ValidEthTransaction;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use alloy_primitives::{hex, B256};
    use monad_chain_config::{revision::ChainRevision, ChainConfig, MockChainConfig};
    use monad_eth_testutil::{generate_consensus_test_block, make_legacy_tx, recover_tx};
    use monad_tfm::base_fee::MIN_BASE_FEE;
    use monad_types::{GENESIS_ROUND, GENESIS_SEQ_NUM};

    use super::{TrackedTxLimits, TrackedTxLimitsConfig};
    use crate::pool::transaction::ValidEthTransaction;

    // pubkey starts with AAA
    const S1: B256 = B256::new(hex!(
        "0ed2e19e3aca1a321349f295837988e9c6f95d4a6fc54cfab6befd5ee82662ad"
    ));

    fn make_test_tx() -> ValidEthTransaction {
        let tx = recover_tx(make_legacy_tx(S1, MIN_BASE_FEE.into(), 1_000_000, 0, 0));

        let last_commit = generate_consensus_test_block(
            GENESIS_ROUND,
            GENESIS_SEQ_NUM,
            MIN_BASE_FEE,
            &MockChainConfig::DEFAULT,
            vec![],
        );

        ValidEthTransaction::validate(
            last_commit.block.header(),
            MockChainConfig::DEFAULT.chain_id(),
            MockChainConfig::DEFAULT
                .get_chain_revision(GENESIS_ROUND)
                .chain_params(),
            MockChainConfig::DEFAULT
                .get_execution_chain_revision(0)
                .execution_chain_params(),
            tx,
            true,
        )
        .unwrap()
    }

    #[test]
    fn basic_exceed_limits() {
        let tx = make_test_tx();

        let mut limits = TrackedTxLimits::new(TrackedTxLimitsConfig {
            max_addresses: 1,
            max_txs: 2,
            soft_tx_expiry: Duration::from_secs(1),
            hard_tx_expiry: Duration::from_secs(5),
        });

        limits
            .prepare_add_tx_to_new_address(tx.clone())
            .expect("Can add new address and tx")
            .finalize();

        assert_eq!(limits.addresses, 1);
        assert_eq!(limits.txs, 1);

        assert!(
            limits.prepare_add_tx_to_new_address(tx.clone()).is_err(),
            "Exceeds max address limit"
        );

        assert_eq!(limits.addresses, 1);
        assert_eq!(limits.txs, 1);

        limits
            .prepare_add_tx_to_existing(tx.clone())
            .expect("Can add another tx because max_txs == 2")
            .finalize();

        assert_eq!(limits.addresses, 1);
        assert_eq!(limits.txs, 2);

        assert!(
            limits.prepare_add_tx_to_existing(tx).is_err(),
            "Exceeds max txs limit"
        );

        assert_eq!(limits.addresses, 1);
        assert_eq!(limits.txs, 2);
    }

    #[test]
    fn new_address() {
        let tx = make_test_tx();

        for max_addresses in [0, 1, 10, 100] {
            let mut limits = TrackedTxLimits::new(TrackedTxLimitsConfig {
                max_addresses,
                max_txs: max_addresses + 1,
                soft_tx_expiry: Duration::from_secs(1),
                hard_tx_expiry: Duration::from_secs(5),
            });

            for _ in 0..max_addresses {
                limits
                    .prepare_add_tx_to_new_address(tx.clone())
                    .expect("Can add new address and tx")
                    .finalize();
            }

            assert_eq!(limits.addresses, max_addresses);
            assert_eq!(limits.txs, max_addresses);

            assert!(
                limits.prepare_add_tx_to_new_address(tx.clone()).is_err(),
                "Cannot add another address because max_addresses hit"
            );

            limits
                .prepare_add_tx_to_existing(tx.clone())
                .expect("Can add one more tx")
                .finalize();

            assert_eq!(limits.addresses, max_addresses);
            assert_eq!(limits.txs, max_addresses + 1);
        }
    }

    #[test]
    fn existing() {
        let tx = make_test_tx();

        for max_txs in [0, 1, 10, 100] {
            let mut limits = TrackedTxLimits::new(TrackedTxLimitsConfig {
                // This use-case is not permitted by the pool but can be used for testing just txs
                max_addresses: 0,
                max_txs,
                soft_tx_expiry: Duration::from_secs(1),
                hard_tx_expiry: Duration::from_secs(5),
            });

            for _ in 0..max_txs {
                limits
                    .prepare_add_tx_to_existing(tx.clone())
                    .expect("Can add new tx")
                    .finalize();
            }

            assert_eq!(limits.addresses, 0);
            assert_eq!(limits.txs, max_txs);

            assert!(
                limits.prepare_add_tx_to_existing(tx.clone()).is_err(),
                "Cannot add another address because max_addresses and max_txs hit"
            );

            assert!(
                limits.prepare_add_tx_to_new_address(tx.clone()).is_err(),
                "Cannot add another address because max_addresses and max_txs hit"
            );

            assert_eq!(limits.addresses, 0);
            assert_eq!(limits.txs, max_txs);
        }
    }

    #[test]
    fn limits_unchanged_until_finalized() {
        let tx = make_test_tx();

        let mut limits = TrackedTxLimits::new(TrackedTxLimitsConfig {
            max_addresses: 2,
            max_txs: 2,
            soft_tx_expiry: Duration::from_secs(1),
            hard_tx_expiry: Duration::from_secs(5),
        });

        // New address

        limits
            .prepare_add_tx_to_new_address(tx.clone())
            .expect("Can add new address and tx");

        assert_eq!(limits.addresses, 0);
        assert_eq!(limits.txs, 0);

        limits
            .prepare_add_tx_to_new_address(tx.clone())
            .expect("Can add new address and tx")
            .finalize();

        assert_eq!(limits.addresses, 1);
        assert_eq!(limits.txs, 1);

        // Existing

        limits
            .prepare_add_tx_to_existing(tx.clone())
            .expect("Can add new tx");

        assert_eq!(limits.addresses, 1);
        assert_eq!(limits.txs, 1);

        limits
            .prepare_add_tx_to_existing(tx)
            .expect("Can add new tx")
            .finalize();

        assert_eq!(limits.addresses, 1);
        assert_eq!(limits.txs, 2);
    }
}
