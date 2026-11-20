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

//! JSON-RPC server of a private domain.
//!
//! A private domain is a chain of its own, whose triedb `monad-domain` writes. Started with
//! `--domain-chain-id`, the server serves that triedb through [`rpc_handler`] in place of
//! [`crate::handlers::rpc_handler`].

pub use self::dispatch::{rpc_handler, DomainRpc};

mod dispatch;

/// Whether `domain_chain_id` is a private domain of the L1 `l1_chain_id`: the L1's chain id
/// in its low 16 bits and a nonzero domain number above them.
pub fn is_domain_of(domain_chain_id: u64, l1_chain_id: u64) -> bool {
    const L1_CHAIN_ID_BITS: u32 = 16;
    (domain_chain_id >> L1_CHAIN_ID_BITS) != 0
        && (domain_chain_id & ((1 << L1_CHAIN_ID_BITS) - 1)) == l1_chain_id
}

#[cfg(test)]
mod tests {
    use monad_chain_config::{
        MONAD_DEVNET_CHAIN_ID, MONAD_MAINNET_CHAIN_ID, MONAD_TESTNET_CHAIN_ID,
    };

    use super::is_domain_of;

    #[test]
    fn domain_chain_id_ends_with_its_l1() {
        assert!(is_domain_of(
            (1 << 16) | MONAD_DEVNET_CHAIN_ID,
            MONAD_DEVNET_CHAIN_ID
        ));
        assert!(is_domain_of(
            (u64::MAX << 16) | MONAD_MAINNET_CHAIN_ID,
            MONAD_MAINNET_CHAIN_ID
        ));

        assert!(!is_domain_of(0, MONAD_DEVNET_CHAIN_ID));
        assert!(!is_domain_of(MONAD_DEVNET_CHAIN_ID, MONAD_DEVNET_CHAIN_ID));
        assert!(!is_domain_of(
            (1 << 16) | MONAD_TESTNET_CHAIN_ID,
            MONAD_DEVNET_CHAIN_ID
        ));
        // 120143 ends with 20143 in decimal, not in its low 16 bits.
        assert!(!is_domain_of(120_143, MONAD_DEVNET_CHAIN_ID));
    }
}
