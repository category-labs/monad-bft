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

mod assembler;
mod assigner;

use std::{collections::HashMap, net::SocketAddr};

use assembler::AssembleMode;
use bytes::Bytes;
use monad_crypto::certificate_signature::{
    CertificateKeyPair as _, CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_types::NodeId;
use rand::seq::SliceRandom as _;

pub(crate) use self::{
    assembler::{assemble, BroadcastType, Chunk, PacketLayout, Recipient},
    assigner::ChunkAssigner,
};
use crate::util::{BuildTarget, Redundancy};

#[derive(Debug)]
pub struct UdpMessage {
    pub dest: SocketAddr,
    pub payload: Bytes,
    pub stride: usize,
}

#[derive(Debug)]
pub enum BuildError {
    // merkle tree depth does not fit in u4
    MerkleTreeTooDeep,
    // chunk id does not fit in u16
    ChunkIdOverflow,
    // failed to create encoder
    EncoderCreationFailed,
    // too many chunks
    TooManyChunks,
    // app message is too large
    AppMessageTooLarge,
    // total stake is zero
    ZeroTotalStake,
    // redundancy is too high
    RedundancyTooHigh,
}

pub(crate) trait PeerAddrLookup<PT: PubKey> {
    fn lookup(&self, node_id: &NodeId<PT>) -> Option<SocketAddr>;
}

// Similar to std::iter::Extend trait but implemented for FnMut as
// well.
pub(crate) trait Collector<T> {
    fn push(&mut self, item: T);
    fn reserve(&mut self, _additional: usize) {}
}

type Result<A, E = BuildError> = std::result::Result<A, E>;

// A compatible interface to crate::udp::build_messages that uses
// packet::assemble underneath.
#[allow(clippy::too_many_arguments)]
pub fn build_messages<ST>(
    key: &ST::KeyPairType,
    segment_size: u16,
    app_message: Bytes,
    redundancy: Redundancy,
    epoch_no: u64,
    unix_ts_ms: u64,
    build_target: BuildTarget<ST>,
    known_addresses: &HashMap<NodeId<CertificateSignaturePubKey<ST>>, SocketAddr>,
    rng: &mut impl rand::Rng,
) -> Vec<(SocketAddr, Bytes)>
where
    ST: CertificateSignatureRecoverable,
{
    use self::assigner::{Partitioned, Replicated};

    let broadcast_type = match build_target {
        BuildTarget::Raptorcast { .. } => BroadcastType::Primary,
        BuildTarget::FullNodeRaptorCast { .. } => BroadcastType::Secondary,
        _ => BroadcastType::Unspecified,
    };
    let peer_lookup = known_addresses;

    let assigner: Box<dyn ChunkAssigner<_>> = match build_target {
        BuildTarget::PointToPoint(to) => Box::new(Replicated::from_unicast(*to)),
        BuildTarget::Broadcast(ref nodes_view) => Box::new(Replicated::from_broadcast(
            nodes_view.iter().copied().collect(),
        )),
        BuildTarget::Raptorcast(ref validators_view) => {
            let mut validator_set: Vec<_> = validators_view
                .iter()
                .map(|(node_id, stake)| (*node_id, stake))
                .collect();
            validator_set.shuffle(rng);
            Box::new(Partitioned::from_validator_set(validator_set))
        }
        BuildTarget::FullNodeRaptorCast(group) => {
            let self_id = NodeId::new(key.pubkey());
            let seed = rng.gen::<usize>();
            let nodes = group
                .iter_skip_self_and_author(&self_id, seed)
                .copied()
                .collect();
            Box::new(Partitioned::from_homogeneous_peers(nodes))
        }
    };

    let app_message_len = app_message.len();
    let mut packets = Vec::new();
    let result = assemble::<ST, _, _>(
        key,
        segment_size as usize,
        app_message,
        redundancy,
        epoch_no,
        unix_ts_ms,
        broadcast_type,
        AssembleMode::GsoFull,
        peer_lookup,
        &*assigner,
        &mut packets,
    );

    let packets = match result {
        Ok(()) => packets,
        Err(BuildError::TooManyChunks) => {
            tracing::error!(
                ?app_message_len,
                ?redundancy,
                ?build_target,
                "Too many chunks generated."
            );
            return vec![];
        }
        Err(BuildError::AppMessageTooLarge) => {
            tracing::error!(?app_message_len, "App message too large");
            return vec![];
        }
        Err(BuildError::ZeroTotalStake) => {
            tracing::error!(?build_target, "Total stake is zero");
            return vec![];
        }
        Err(BuildError::RedundancyTooHigh) => {
            tracing::error!(?redundancy, ?build_target, "Redundancy too high");
            return vec![];
        }
        Err(e) => {
            tracing::error!("Failed to build packets: {:?}", e);
            return vec![];
        }
    };

    packets
        .into_iter()
        .map(|msg| (msg.dest, msg.payload))
        .collect()
}

impl<PT: PubKey> PeerAddrLookup<PT> for HashMap<NodeId<PT>, SocketAddr> {
    fn lookup(&self, node_id: &NodeId<PT>) -> Option<SocketAddr> {
        self.get(node_id).copied()
    }
}

impl<T> Collector<T> for Vec<T> {
    fn push(&mut self, item: T) {
        Vec::push(self, item)
    }

    fn reserve(&mut self, additional: usize) {
        Vec::reserve(self, additional)
    }
}

impl<F, T> Collector<T> for F
where
    F: FnMut(T),
{
    fn push(&mut self, item: T) {
        self(item)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use bytes::{Bytes, BytesMut};
    use monad_crypto::certificate_signature::CertificateSignature;
    use monad_secp::SecpSignature;
    use monad_testutil::signing::get_key;
    use monad_types::{Round, RoundSpan, Stake};
    use rand::{rngs::StdRng, seq::IteratorRandom as _, Rng as _, RngCore as _, SeedableRng as _};

    use super::{
        assembler::{CHUNK_HEADER_LEN, HEADER_LEN},
        build_messages as build_new, *,
    };
    use crate::{
        udp::build_messages_with_rng as build_old,
        util::{EpochValidators, Group},
        SIGNATURE_SIZE,
    };

    const DEFAULT_SEGMENT_LEN: usize = 1400;
    const DEFAULT_PROOF_LEN: usize = 100; // 5 * 20
    const DEFAULT_DATA_LEN: usize =
        DEFAULT_SEGMENT_LEN - HEADER_LEN - CHUNK_HEADER_LEN - DEFAULT_PROOF_LEN;

    const EPOCH: u64 = 5;
    const UNIX_TS_MS: u64 = 5;

    type ST = SecpSignature;
    type PT = CertificateSignaturePubKey<ST>;

    fn key_pair(seed: u64) -> <ST as CertificateSignature>::KeyPairType {
        get_key::<ST>(seed)
    }

    fn node_id(seed: u64) -> NodeId<PT> {
        let key_pair = get_key::<ST>(seed);
        NodeId::new(key_pair.pubkey())
    }

    #[test]
    fn test_compatibility() {
        let rng = &mut rand::thread_rng();

        let node_addr = |i: u64| {
            SocketAddr::from((
                ((i & 0xffffffff0000 >> 16) as u32).to_le_bytes(),
                (i & 0xffff) as u16,
            ))
        };
        let rand_stake = || Stake::from(rand::thread_rng().gen_range(1..1000000u64));
        let rand_validators = |n: usize| EpochValidators {
            validators: (1..n).map(|i| (node_id(i as u64), rand_stake())).collect(),
        };
        let known_addresses = (1..5).map(|i| (node_id(i), node_addr(i))).collect();

        for num_pkts in [0, 1, 100, 2000] {
            let mut trail_lens = (2..(DEFAULT_SEGMENT_LEN - 1)).choose_multiple(rng, 10);
            // always test for these boundary cases.
            trail_lens.push(0);
            trail_lens.push(1);
            trail_lens.push(DEFAULT_DATA_LEN - 1);
            trail_lens.push(DEFAULT_DATA_LEN);
            trail_lens.push(DEFAULT_DATA_LEN + 1);
            trail_lens.push(DEFAULT_SEGMENT_LEN - 1);
            trail_lens.push(DEFAULT_SEGMENT_LEN);
            trail_lens.push(DEFAULT_SEGMENT_LEN + 1);

            for trail_len in trail_lens {
                for redundancy in [1.0, 1.3, 3.0, 7.0] {
                    let app_msg_len = DEFAULT_SEGMENT_LEN * num_pkts + trail_len;
                    let mut app_msg = BytesMut::zeroed(app_msg_len);
                    rng.fill_bytes(&mut app_msg);
                    let app_msg = app_msg.freeze();

                    // validator #5 and up will have no known addresses
                    let num_validators = rng.gen_range(2..8);
                    let validators = rand_validators(num_validators);
                    let self_id = (1..(num_validators + 1)).choose(rng).unwrap() as u64;
                    let self_node_id = node_id(self_id);
                    let self_key = key_pair(self_id);

                    let assert_compatible = |target| {
                        assert_build_compatible(
                            &self_key,
                            &app_msg,
                            redundancy,
                            &target,
                            &known_addresses,
                        )
                    };

                    {
                        let epoch_validators = validators.view_without(vec![&self_node_id]);
                        let build_target = BuildTarget::Raptorcast(epoch_validators);
                        assert_compatible(build_target);
                    }

                    // skip broadcast cases that are too slow to test on ci
                    if num_pkts < 30 && redundancy < 3.0 {
                        let epoch_validators = validators.view_without(vec![&self_node_id]);
                        let build_target = BuildTarget::Broadcast(epoch_validators.into());
                        assert_compatible(build_target);
                    }

                    for node in validators.validators.keys() {
                        let build_target = BuildTarget::PointToPoint(node);
                        assert_compatible(build_target);
                    }

                    {
                        // we pretend the validators form a fullnode group
                        let group = Group::new_fullnode_group(
                            validators.validators.keys().cloned().collect(),
                            &self_node_id,
                            self_node_id,
                            RoundSpan::single(Round(1)).unwrap(),
                        );
                        let build_target = BuildTarget::FullNodeRaptorCast(&group);
                        assert_compatible(build_target);
                    }
                }
            }
        }
    }

    fn assert_build_compatible(
        key: &<ST as CertificateSignature>::KeyPairType,
        app_msg: &Bytes,
        redundancy: f32,
        build_target: &BuildTarget<ST>,
        known_addresses: &HashMap<NodeId<PT>, SocketAddr>,
    ) {
        let make_rng = || StdRng::seed_from_u64(42);
        let redundancy = Redundancy::from_f32(redundancy).expect("bad redundancy");

        let new = build_new(
            key,
            DEFAULT_SEGMENT_LEN as u16,
            app_msg.clone(),
            redundancy,
            EPOCH,
            UNIX_TS_MS,
            build_target.clone(),
            known_addresses,
            &mut make_rng(),
        );

        let old = build_old(
            key,
            DEFAULT_SEGMENT_LEN as u16,
            app_msg.clone(),
            redundancy,
            EPOCH,
            UNIX_TS_MS,
            build_target.clone(),
            known_addresses,
            &mut make_rng(),
        );

        assert_compatible_eq(&old, &new);
    }

    type Packets = Vec<(SocketAddr, Bytes)>;

    // We loosen up two constraints for comparing the output from old
    // and new implementations:
    //
    // 1. we allow the packets to be reordered (new implementation
    // uses a HashMap when assembling packets, which scrambles the
    // order.)
    //
    // 2. we allow the signature to differ because the signature is
    // non-deterministic.
    fn normalize(packets: &Packets) -> Packets {
        let mut normalized_packets = BTreeSet::new();
        for (addr, payload) in packets {
            let payload_without_signature = payload.slice(SIGNATURE_SIZE..);
            normalized_packets.insert((*addr, payload_without_signature));
        }

        normalized_packets.into_iter().collect()
    }

    #[allow(unreachable_code)]
    fn assert_compatible_eq(expected: &Packets, actual: &Packets) {
        assert_eq!(expected.len(), actual.len());

        // Set the environment variable to show detailed difference in
        // packet bytes.
        let (expected, actual) = (normalize(expected), normalize(actual));
        if option_env!("SCRUTINIZE_PACKET") != Some("1") {
            assert_eq!(expected, actual);
            return;
        }

        for ((e_addr, e_payload), (a_addr, a_payload)) in expected.iter().zip(actual.iter()) {
            // The recipient's socket address is the same.
            if e_addr != a_addr {
                println!("{:?} {:?}", e_addr, a_addr);
                println!("{:?}\n-----\n{:?}", e_payload, a_payload);
                assert_eq!(e_addr, a_addr);
            }

            // The signatures are different even when signing the same
            // message with the same key.
            assert_eq!(e_payload.len(), a_payload.len());

            for (i, (be, ba)) in e_payload.iter().zip(a_payload.iter()).enumerate() {
                assert_eq!(
                    be, ba,
                    "payloads differ at byte {}: 0x{:02x} != 0x{:02x}",
                    i, be, ba
                );
            }
        }
    }
}
