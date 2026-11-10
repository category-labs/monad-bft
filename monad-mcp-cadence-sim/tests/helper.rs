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

use monad_mcp_cadence::stub::{CadenceRuntime, CadenceWireMsg, Conductor, SlotConsensus};
use monad_mcp_cadence_sim::CadenceSwarm;
use monad_mcp_common::{Slot, Timestamp, stub::NodeId};

pub fn expect_finalized<S, C>(
    swarm: &CadenceSwarm<S, C>,
    node: NodeId,
    slots: impl IntoIterator<Item = u64>,
) where
    S: SlotConsensus + 'static,
    C: Conductor + 'static,
    C::Message: Clone,
    CadenceRuntime<S, C>: Send,
    CadenceWireMsg<S, C>: Send,
{
    let expected: Vec<Slot> = slots.into_iter().map(Slot).collect();
    assert_eq!(swarm.log().get_finalized_slots(node), expected);
}

pub fn expect_finalized_at<S, C>(
    swarm: &CadenceSwarm<S, C>,
    node: NodeId,
    timestamps: impl IntoIterator<Item = Timestamp>,
) where
    S: SlotConsensus + 'static,
    C: Conductor + 'static,
    C::Message: Clone,
    CadenceRuntime<S, C>: Send,
    CadenceWireMsg<S, C>: Send,
{
    let expected: Vec<Timestamp> = timestamps.into_iter().collect();
    let finalized: Vec<Timestamp> = swarm.log().get_finalization_times(node);
    assert_eq!(finalized, expected);
}

/// The inputs the fast path would have formed, for the fallback tests
// each test crate that includes this file uses a different part of it
#[allow(dead_code)]
pub mod fixtures {
    use std::{collections::HashMap, sync::Arc};

    use cadence::{
        env::{D33, EncodingScheme, MerkleHash, MerkleRoot, ProposalHeader as Header},
        slot::fallback::{
            CertifiedEntry, EnterFallbackCert, EnterFallbackVote, Entry, FallbackEntry, Metablock,
            Mvba, monad_mvba::MvbaContext,
        },
        types::{
            HeaderAuth, IsVote, ProposalId, StrongQc, ValidatorData, VoteMsg, VotePool, WeakQc,
        },
    };
    use monad_mcp_cadence::{spec::ProposalHeader as _, stub as cadence};
    use monad_mcp_common::{
        Lane, Lanes, ProposalScope, Slot, TimestampDelta,
        spec::KeyPair as _,
        stub::{NodeId, Stake},
    };

    const NUM_NODES: u64 = 4;

    /// The pid of a header whose root spells out `n`, so distinct seeds give distinct pids
    fn pid(n: u64) -> ProposalId {
        let mut hash = [0u8; 20];
        hash[..8].copy_from_slice(&n.to_le_bytes());
        let header = Header {
            root: MerkleRoot(MerkleHash(hash)),
            scheme: EncodingScheme::D33(D33 {
                slot: SLOT,
                lane: Lane::new_unchecked(0),
                depth: 3,
                msg_len: 1,
                unix_ts: 0,
            }),
        };
        header.pid()
    }
    const NUM_LANES: usize = 2;
    pub const SLOT: Slot = Slot(0);

    pub fn nodes() -> Vec<NodeId> {
        (0..NUM_NODES).map(NodeId::dummy).collect()
    }

    /// A supermajority of the four: three signers
    fn quorum() -> Vec<NodeId> {
        nodes()[..3].to_vec()
    }

    pub fn validator_data() -> Arc<ValidatorData> {
        let valset: HashMap<_, _> = nodes()
            .into_iter()
            .map(|node| (node, Stake::from(1u64)))
            .collect();
        let mapping = nodes()
            .into_iter()
            .map(|node| (node, node.keypair().pubkey()))
            .collect();

        Arc::new(ValidatorData::new(valset, mapping))
    }

    /// The same round-robin the implementation uses, restated so the tests do
    /// not take their expectations from the code under test
    /// TODO: wire up actual leader election
    pub fn leader_of(view: u64) -> NodeId {
        let index = (SLOT.get() + view) % NUM_NODES;
        nodes()[index as usize]
    }

    /// An instance for `node`, of whichever fallback protocol the caller runs
    pub fn mvba<M, V>(node: NodeId, validator_data: &Arc<ValidatorData>, delta: TimestampDelta) -> M
    where
        M: Mvba<V, Context = MvbaContext>,
        V: cadence::slot::fallback::ValidateInput + cadence::slot::fallback::Votable,
    {
        M::new(MvbaContext {
            slot: SLOT,
            num_lanes: NUM_LANES,
            node_id: node,
            key: Arc::new(node.keypair()),
            validator_data: validator_data.clone(),
            header_auth: Arc::new(HeaderAuth::new(|_| false)),
            delta,
        })
    }

    /// A valid metablock whose entries are determined by `seed`, so two seeds
    /// give two metablocks that cannot be confused for one another
    pub fn fast_metablock(seed: u64, validator_data: &ValidatorData) -> Metablock {
        Metablock::new(Lanes::new(NUM_LANES, |j| {
            let entry = Entry::Positive(pid(seed * 100 + usize::from(j) as u64));
            CertifiedEntry::FastQc(strong_qc(
                ProposalScope::new(SLOT, j),
                entry,
                &quorum(),
                validator_data,
            ))
        }))
    }

    /// Valid but *not* a fast metablock: its first entry rests on a fallback
    /// quorum. `entries()` matches [`fast_metablock`] on the same seed, so the
    /// two differ only in their evidence
    pub fn mixed_evidence_metablock(seed: u64, validator_data: &ValidatorData) -> Metablock {
        Metablock::new(Lanes::new(NUM_LANES, |j| {
            let entry = Entry::Positive(pid(seed * 100 + usize::from(j) as u64));
            if j == Lane::new_unchecked(0) {
                CertifiedEntry::FallbackQc(weak_qc(
                    ProposalScope::new(SLOT, j),
                    FallbackEntry(entry),
                    &quorum(),
                    validator_data,
                ))
            } else {
                CertifiedEntry::FastQc(strong_qc(
                    ProposalScope::new(SLOT, j),
                    entry,
                    &quorum(),
                    validator_data,
                ))
            }
        }))
    }

    /// A genuine fallback certificate for this slot
    pub fn enter_fallback_cert(validator_data: &ValidatorData) -> EnterFallbackCert {
        strong_qc(SLOT, EnterFallbackVote, &quorum(), validator_data)
    }

    /// A fast input as the slot fallback takes it: a fast metablock carries no
    /// fallback certificate
    pub fn fast_input(
        seed: u64,
        validator_data: &ValidatorData,
    ) -> (Metablock, Option<EnterFallbackCert>) {
        (fast_metablock(seed, validator_data), None)
    }

    /// A non-fast input as the slot fallback takes it: the metablock with the
    /// fallback certificate it is valid only beside
    pub fn non_fast_input(
        seed: u64,
        validator_data: &ValidatorData,
    ) -> (Metablock, Option<EnterFallbackCert>) {
        (
            mixed_evidence_metablock(seed, validator_data),
            Some(enter_fallback_cert(validator_data)),
        )
    }

    pub fn vote_pool<V: IsVote>(scope: V::Scope, vote: V, signers: &[NodeId]) -> VotePool<V> {
        use monad_mcp_cadence::spec::validator::ValidatorData as _;

        let validator_data = validator_data();
        let mut pool = VotePool::new(scope.clone());
        for node in signers {
            pool.add_vote(
                validator_data.index_of(node).expect("a validator"),
                VoteMsg::new_signed(scope.clone(), vote.clone(), &node.keypair()),
            );
        }
        pool
    }

    /// Aggregate a genuine supermajority certificate over `vote`
    pub fn strong_qc<V: IsVote>(
        scope: V::Scope,
        vote: V,
        signers: &[NodeId],
        validator_data: &ValidatorData,
    ) -> StrongQc<V> {
        vote_pool(scope, vote, signers)
            .tally(validator_data)
            .strong_qc()
            .expect("the signers hold a supermajority of stake")
    }

    /// Aggregate a genuine `f+1` certificate over `vote`
    fn weak_qc<V: IsVote>(
        scope: V::Scope,
        vote: V,
        signers: &[NodeId],
        validator_data: &ValidatorData,
    ) -> WeakQc<V> {
        let pool = vote_pool(scope, vote, signers);
        let mut qcs = pool.tally(validator_data).weak_qcs().into_iter();
        match (qcs.next(), qcs.next()) {
            (Some(qc), None) => qc,
            (None, _) => panic!("the signers hold more than an honest threshold of stake"),
            (Some(_), Some(_)) => panic!("the signers all voted the same way"),
        }
    }
}
