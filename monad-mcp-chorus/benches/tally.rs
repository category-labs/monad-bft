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

//! A vote pool's cost at realistic validator counts: filling it, and one
//! tally of a full pool (aggregation, signature checks, signer extraction,
//! stake sum). Every vote triggers a tally, so a slot costs about n tallies
//! of a growing pool.

use std::collections::HashMap;

use alloy_rlp::{RlpDecodableWrapper, RlpEncodableWrapper};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use monad_mcp_chorus::{
    spec::vote::SigningDomain,
    stub::types::{IsVote, ValidatorData, VoteMsg, VotePool},
};
use monad_mcp_common::{
    spec::KeyPair as _,
    stub::{NodeId, Stake},
};

#[derive(Clone, PartialEq, Eq, Hash, Debug, RlpEncodableWrapper, RlpDecodableWrapper)]
struct Claim(u64);

struct ClaimDomain;

impl SigningDomain for ClaimDomain {
    const PREFIX: &'static [u8] = b"\x1Cmonad/cadence/bench-claim/1\n";
}

impl IsVote for Claim {
    type Scope = u64;
    type SigningDomain = ClaimDomain;
}

const SCOPE: u64 = 7;

fn setup(n: u64) -> (ValidatorData, Vec<(NodeId, VoteMsg<Claim>)>) {
    let nodes: Vec<NodeId> = (0..n).map(NodeId::dummy).collect();
    let valset: HashMap<_, _> = nodes.iter().map(|node| (*node, Stake::from(1))).collect();
    let mapping: HashMap<_, _> = nodes
        .iter()
        .map(|node| (*node, node.keypair().pubkey()))
        .collect();
    let validator_data = ValidatorData::new(valset, mapping);
    // in arrival order unrelated to the index order, and one in ten voting
    // for a rival, so a tally sees two buckets
    let mut votes: Vec<_> = nodes
        .iter()
        .map(|node| {
            let claim = Claim(u64::from(u64::from(*node) % 10 == 0));
            (*node, VoteMsg::new_signed(SCOPE, claim, &node.keypair()))
        })
        .collect();
    votes.reverse();
    (validator_data, votes)
}

fn fill(votes: &[(NodeId, VoteMsg<Claim>)]) -> VotePool<Claim> {
    let mut pool = VotePool::new(SCOPE);
    for (node, vote) in votes {
        pool.add_vote(*node, vote.clone());
    }
    pool
}

fn bench(c: &mut Criterion) {
    for n in [200, 1500] {
        let (validator_data, votes) = setup(n);

        c.bench_with_input(BenchmarkId::new("fill", n), &votes, |b, votes| {
            b.iter(|| fill(votes))
        });

        let pool = fill(&votes);
        c.bench_with_input(BenchmarkId::new("tally", n), &pool, |b, pool| {
            b.iter(|| pool.tally(&validator_data).strong_qc())
        });
    }
}

criterion_group!(benches, bench);
criterion_main!(benches);
