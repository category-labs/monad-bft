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

//! The Eth execution relation (real transactions) driven through the `monad-sim`
//! harness, mirroring the `nonces`/`reserve_balance` engine tests. This is the
//! highest-coverage test category and the main thing the new engine must host
//! before the legacy `Nodes` engine can be retired.

mod eth_swarm_common;

#[cfg(test)]
mod test {
    use alloy_eips::eip2718::Encodable2718;
    use alloy_primitives::B256;
    use monad_eth_testutil::{make_legacy_tx, secret_to_eth_address};
    use monad_mock_swarm::sim::{Network, SimSwarm};
    use monad_sim::{time::secs, Time};
    use monad_updaters::txpool::ByzantineConfig;

    use crate::eth_swarm_common::{
        eth_swarm_config, verify_transactions_in_sim, BASE_FEE, GAS_LIMIT,
    };

    #[test]
    fn eth_transactions_commit_through_monad_sim() {
        let sender = B256::repeat_byte(15);
        let config = eth_swarm_config(2, vec![secret_to_eth_address(sender)], |_| {
            ByzantineConfig::default()
        });
        let mut swarm = SimSwarm::from_builders(0, config, |_| Network::default());
        let node = swarm.node_ids()[0];

        let deadline = Time(0) + secs(20);
        // Step past initial sync until the nodes are committing blocks.
        assert!(swarm.run_until_blocks(1, deadline), "no initial progress");

        let mut expected = Vec::new();
        for nonce in 0..5 {
            let tx = make_legacy_tx(sender, BASE_FEE, GAS_LIMIT, nonce, 10);
            swarm.send_transaction(node, tx.encoded_2718().into());
            expected.push(tx);
        }

        assert!(
            swarm.run_until_blocks(5, deadline),
            "stalled before committing txns"
        );
        assert!(verify_transactions_in_sim(&swarm, expected));
        swarm.assert_agreement();
    }
}
