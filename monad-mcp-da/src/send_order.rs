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

use monad_mcp_common::containers::OrderKey;

use super::types::{NodeId, ProposalId};

// shuffle a sender's messages about one proposal into a pseudo-random order:
// uniform over proposals and senders, so no node is systematically served or
// asked first, and a function of (pid, sender) alone, so it is the same in
// every run, process and release.
//
// todo: replace with the send strategy once sends are prioritized.
pub(crate) fn shuffle_sends<T>(items: &mut [T], pid: &ProposalId, sender: &NodeId) {
    OrderKey::new(b"monad-mcp-da send order")
        .with(pid)
        .with(sender)
        .shuffle(items);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_send_order_is_a_permutation_fixed_by_proposal_and_sender() {
        use super::super::test_util::{epoch_handle, proposal_chunks};

        let pid = |payload| -> ProposalId {
            use monad_mcp_cadence::spec::ProposalHeader as _;
            proposal_chunks(&epoch_handle(), payload).0.pid()
        };
        let order = |pid: &ProposalId, sender: u64| {
            let mut nodes: Vec<u64> = (0..16).collect();
            shuffle_sends(&mut nodes, pid, &NodeId::dummy(sender));
            nodes
        };

        let (a, b) = (pid(1), pid(2));
        let mut sorted = order(&a, 0);
        sorted.sort();
        assert_eq!(sorted, (0..16).collect::<Vec<_>>());
        assert_eq!(order(&a, 0), order(&a, 0));
        assert_ne!(order(&a, 0), order(&a, 1));
        assert_ne!(order(&a, 0), order(&b, 0));
        // pinned: blake3, rlp, ChaCha20 and the stub pid (rapidhash v3) are stable,
        // so the order is the same across processes and releases
        assert_eq!(order(&a, 0), PINNED);
    }

    const PINNED: [u64; 16] = [3, 15, 10, 9, 8, 0, 13, 11, 6, 14, 12, 7, 2, 1, 4, 5];
}
