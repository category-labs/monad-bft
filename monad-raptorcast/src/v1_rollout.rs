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

use monad_crypto::certificate_signature::PubKey;
pub use monad_node_config::raptorcast::{DeterministicProtocolRolloutStage, CURRENT_STAGE};

use crate::udp::{ChunkVersion, ValidatedChunk};

// should only be called on receiving a v0 or v1 raptorcast chunk
pub(crate) fn should_accept<PT: PubKey>(
    stage: DeterministicProtocolRolloutStage,
    chunk: &ValidatedChunk<PT>,
) -> bool {
    match chunk.version {
        ChunkVersion::V1 => should_accept_v1(stage),
        ChunkVersion::V0 => should_accept_v0(stage),
    }
}

fn should_accept_v0(stage: DeterministicProtocolRolloutStage) -> bool {
    !matches!(stage, DeterministicProtocolRolloutStage::AlwaysV1)
}

fn should_accept_v1(stage: DeterministicProtocolRolloutStage) -> bool {
    !matches!(stage, DeterministicProtocolRolloutStage::AlwaysV0)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        net::{IpAddr, Ipv4Addr, SocketAddr},
    };

    use bytes::Bytes;
    use itertools::Itertools as _;
    use monad_crypto::{
        certificate_signature::CertificateSignaturePubKey,
        hasher::{Hasher, HasherType},
    };
    use monad_secp::{KeyPair, SecpSignature};
    use monad_types::{Epoch, NodeId, Round, RoundSpan, Stake};
    use monad_validator::validator_set::{ValidatorSet, ValidatorSetType as _};

    use super::{should_accept, DeterministicProtocolRolloutStage};
    use crate::{
        auth::AuthRecvMsg,
        packet::MessageBuilder,
        udp::{ChunkVersion, GroupId, UdpState, ValidatedChunk},
        util::{
            BroadcastMode, BuildTarget, EncodingScheme, FullNodeGroupMap, HexBytes,
            PrimaryBroadcastGroup, Redundancy, SecondaryBroadcastGroup, SecondaryGroup,
            SecondaryGroupAssignment, StubProposerSchedule,
        },
    };

    type SignatureType = SecpSignature;
    type PubKeyType = CertificateSignaturePubKey<SignatureType>;

    const EPOCH: Epoch = Epoch(5);
    const ROUND: Round = Round(42);

    // (stage, accepts v0, accepts v1)
    fn accept_by_stage() -> [(DeterministicProtocolRolloutStage, bool, bool); 4] {
        use DeterministicProtocolRolloutStage::*;
        [
            (AlwaysV0, true, false),
            (AcceptBothPublishV0, true, true),
            (AcceptBothPublishV1, true, true),
            (AlwaysV1, false, true),
        ]
    }

    fn stub_chunk(version: ChunkVersion) -> ValidatedChunk<PubKeyType> {
        ValidatedChunk {
            chunk: Bytes::new(),
            message: Bytes::new(),
            signature: Bytes::new(),
            author: NodeId::new(make_key_pair(0).pubkey()),
            group_id: GroupId::Primary(EPOCH),
            unix_ts_ms: 0,
            app_message_hash: None,
            merkle_root: HexBytes([0; 20]),
            app_message_len: 0,
            version,
            encoding_scheme: EncodingScheme::Unspecified,
            broadcast_mode: BroadcastMode::Primary,
            recipient_hash: None,
            chunk_id: 0,
            num_source_symbols: 0,
            encoded_symbol_capacity: 0,
        }
    }

    #[test]
    fn test_should_accept_by_stage() {
        for (stage, accepts_v0, accepts_v1) in accept_by_stage() {
            assert_eq!(
                should_accept(stage, &stub_chunk(ChunkVersion::V0)),
                accepts_v0
            );
            assert_eq!(
                should_accept(stage, &stub_chunk(ChunkVersion::V1)),
                accepts_v1
            );
        }
    }

    fn make_key_pair(n: u8) -> KeyPair {
        let mut hasher = HasherType::new();
        hasher.update(n.to_le_bytes());
        let mut hash = hasher.hash();
        KeyPair::from_bytes(&mut hash.0).unwrap()
    }

    fn validator_set() -> (KeyPair, ValidatorSet<PubKeyType>) {
        let mut keys = (0_u8..100).map(make_key_pair).collect_vec();
        let valset = keys
            .iter()
            .map(|key| (NodeId::new(key.pubkey()), Stake::ONE))
            .collect();
        let validators = ValidatorSet::new_unchecked(valset);
        (keys.pop().unwrap(), validators)
    }

    // returns the number of successfully decoded messages.
    fn primary_publish_and_receive(receiver_stage: DeterministicProtocolRolloutStage) -> usize {
        let (sender_key, validators) = validator_set();
        let sender_id = NodeId::new(sender_key.pubkey());
        let group_map = [(EPOCH, validators.clone())].into();
        let fn_group_map = FullNodeGroupMap::default();
        let proposer_schedule = StubProposerSchedule::VALID;
        let group = PrimaryBroadcastGroup::of_epoch(EPOCH, &sender_id, &group_map).unwrap();

        let app_message: Bytes = vec![0xAB_u8; 64 * 1024].into();

        let build_target = BuildTarget::raptorcast(group, ROUND);
        let packets = MessageBuilder::<SignatureType>::new(&sender_key)
            .redundancy(Redundancy::from_u8(2))
            .build_vec(&app_message, &build_target)
            .expect("build should succeed");
        assert!(!packets.is_empty());

        // Set up receiver
        let receiver_id = validators
            .get_members()
            .keys()
            .find(|id| **id != sender_id)
            .copied()
            .unwrap();

        let mut udp_state = UdpState::<SignatureType>::new(receiver_id, u64::MAX, 10_000);
        udp_state.set_v1_rollout(receiver_stage);

        // Feed all packets into the receiver
        let mut decoded_count = 0;
        for packet in &packets {
            let recv_msg = AuthRecvMsg {
                src_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8000),
                payload: packet.payload.clone(),
                stride: packet.stride as u16,
                sender: None,
            };
            let decoded = udp_state.handle_message(
                &group_map,
                &fn_group_map,
                &proposer_schedule,
                |_, _, _| {},
                recv_msg,
            );
            decoded_count += decoded.len();
        }

        decoded_count
    }

    #[test]
    fn test_primary_decodes_by_stage() {
        for (stage, _accepts_v0, accepts_v1) in accept_by_stage() {
            assert_eq!(primary_publish_and_receive(stage), usize::from(accepts_v1));
        }
    }

    // Secondary analogue of primary_publish_and_receive: publisher is
    // a validator, receiver is a full node in the publisher's
    // secondary group.
    fn secondary_publish_and_receive(receiver_stage: DeterministicProtocolRolloutStage) -> usize {
        let publisher_key = make_key_pair(0);
        let publisher_id = NodeId::new(publisher_key.pubkey());

        let full_nodes: BTreeSet<NodeId<PubKeyType>> = (1_u8..=20)
            .map(|n| NodeId::new(make_key_pair(n).pubkey()))
            .collect();
        let group = SecondaryGroup::new(full_nodes.clone()).unwrap();

        let bg = SecondaryBroadcastGroup::as_publisher(&publisher_id, ROUND, &group);
        let build_target = BuildTarget::fullnode_raptorcast(bg, EPOCH);

        let app_message: Bytes = vec![0xAB_u8; 64 * 1024].into();
        let packets = MessageBuilder::<SignatureType>::new(&publisher_key)
            .redundancy(Redundancy::from_u8(2))
            .build_vec(&app_message, &build_target)
            .expect("build should succeed");
        assert!(!packets.is_empty());

        // Set up receiver (a full node in the group).
        let receiver_id = *full_nodes.iter().next().unwrap();
        let group_map: std::collections::BTreeMap<_, _> = std::collections::BTreeMap::new();
        let mut fn_group_map = FullNodeGroupMap::<PubKeyType>::default();
        let round_span = RoundSpan::new(Round(0), Round(u64::MAX)).unwrap();
        fn_group_map
            .try_insert(SecondaryGroupAssignment::new(
                publisher_id,
                round_span,
                group.clone(),
            ))
            .expect("group insert");

        let mut udp_state = UdpState::<SignatureType>::new(receiver_id, u64::MAX, 10_000);
        udp_state.set_v1_rollout(receiver_stage);

        let proposer_map = StubProposerSchedule::default();
        let mut decoded_count = 0;
        for packet in &packets {
            let recv_msg = AuthRecvMsg {
                src_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8000),
                payload: packet.payload.clone(),
                stride: packet.stride as u16,
                sender: None,
            };
            let decoded = udp_state.handle_message(
                &group_map,
                &fn_group_map,
                &proposer_map,
                |_, _, _| {},
                recv_msg,
            );
            decoded_count += decoded.len();
        }
        decoded_count
    }

    #[test]
    fn test_secondary_decodes_by_stage() {
        for (stage, _accepts_v0, accepts_v1) in accept_by_stage() {
            assert_eq!(
                secondary_publish_and_receive(stage),
                usize::from(accepts_v1)
            );
        }
    }

    #[test]
    fn test_v1_secondary_rebroadcast_matches_deterministic_assignment() {
        use crate::{packet::deterministic::SecondaryEncoding, util::EncodingScheme};

        let publisher_key = make_key_pair(0);
        let publisher_id = NodeId::new(publisher_key.pubkey());

        let full_nodes: BTreeSet<NodeId<PubKeyType>> = (1_u8..=20)
            .map(|n| NodeId::new(make_key_pair(n).pubkey()))
            .collect();
        let group = SecondaryGroup::new(full_nodes.clone()).unwrap();

        let bg = SecondaryBroadcastGroup::as_publisher(&publisher_id, ROUND, &group);
        let build_target = BuildTarget::fullnode_raptorcast(bg, EPOCH);

        let app_message: Bytes = vec![0xCD_u8; 64 * 1024].into();
        let unix_ts_ms = 1_700_000_000_000_u64;
        let packets = MessageBuilder::<SignatureType>::new(&publisher_key)
            .unix_ts_ms(unix_ts_ms)
            .redundancy(Redundancy::from_u8(2))
            .build_vec(&app_message, &build_target)
            .expect("build should succeed");
        assert!(!packets.is_empty());

        let receiver_id = *full_nodes.iter().next().unwrap();
        let mut fn_group_map = FullNodeGroupMap::<PubKeyType>::default();
        let round_span = RoundSpan::new(Round(0), Round(u64::MAX)).unwrap();
        fn_group_map
            .try_insert(SecondaryGroupAssignment::new(
                publisher_id,
                round_span,
                group.clone(),
            ))
            .expect("group insert");

        let mut udp_state = UdpState::<SignatureType>::new(receiver_id, u64::MAX, 10_000);
        udp_state.set_v1_rollout(DeterministicProtocolRolloutStage::AlwaysV1);

        let group_map: std::collections::BTreeMap<_, _> = std::collections::BTreeMap::new();
        let proposer_map = StubProposerSchedule::default();
        let mut rebroadcast_count = 0usize;
        for packet in &packets {
            let recv_msg = AuthRecvMsg {
                src_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8000),
                payload: packet.payload.clone(),
                stride: packet.stride as u16,
                sender: None,
            };
            udp_state.handle_message(
                &group_map,
                &fn_group_map,
                &proposer_map,
                |_targets, _payload, _stride| rebroadcast_count += 1,
                recv_msg,
            );
        }

        // Reconstruct the deterministic chunk assignment to count how
        // many chunks were addressed to the receiver.
        let bg2 = SecondaryBroadcastGroup::as_publisher(&publisher_id, ROUND, &group);
        let encoding = SecondaryEncoding::new(
            EncodingScheme::Deterministic25(ROUND),
            &bg2,
            app_message.len(),
            unix_ts_ms,
        )
        .unwrap();
        let chunks = encoding.make_chunks().unwrap();
        let addressed_to_receiver = chunks
            .iter()
            .filter(|c| *c.recipient().node_id() == receiver_id)
            .count();

        assert_eq!(rebroadcast_count, addressed_to_receiver);
        assert!(
            rebroadcast_count > 0,
            "receiver should be assigned some chunks"
        );
    }

    #[test]
    fn test_v1_secondary_conflicting_commitment_rejected() {
        let publisher_key = make_key_pair(0);
        let publisher_id = NodeId::new(publisher_key.pubkey());

        let full_nodes: BTreeSet<NodeId<PubKeyType>> = (1_u8..=20)
            .map(|n| NodeId::new(make_key_pair(n).pubkey()))
            .collect();
        let group = SecondaryGroup::new(full_nodes.clone()).unwrap();

        let bg1 = SecondaryBroadcastGroup::as_publisher(&publisher_id, ROUND, &group);
        let build_target_a = BuildTarget::fullnode_raptorcast(bg1, EPOCH);
        let bg2 = SecondaryBroadcastGroup::as_publisher(&publisher_id, ROUND, &group);
        let build_target_b = BuildTarget::fullnode_raptorcast(bg2, EPOCH);

        // Two different messages at the same round, both signed by the
        // same publisher: the second must not decode.
        let app_a: Bytes = vec![0xAA_u8; 64 * 1024].into();
        let app_b: Bytes = vec![0xBB_u8; 64 * 1024].into();
        let unix_ts_ms = 1_700_000_000_000_u64;

        let packets_a = MessageBuilder::<SignatureType>::new(&publisher_key)
            .unix_ts_ms(unix_ts_ms)
            .redundancy(Redundancy::from_u8(2))
            .build_vec(&app_a, &build_target_a)
            .expect("build a");
        let packets_b = MessageBuilder::<SignatureType>::new(&publisher_key)
            .unix_ts_ms(unix_ts_ms)
            .redundancy(Redundancy::from_u8(2))
            .build_vec(&app_b, &build_target_b)
            .expect("build b");

        let receiver_id = *full_nodes.iter().next().unwrap();
        let mut fn_group_map = FullNodeGroupMap::<PubKeyType>::default();
        let round_span = RoundSpan::new(Round(0), Round(u64::MAX)).unwrap();
        fn_group_map
            .try_insert(SecondaryGroupAssignment::new(
                publisher_id,
                round_span,
                group.clone(),
            ))
            .expect("group insert");

        let mut udp_state = UdpState::<SignatureType>::new(receiver_id, u64::MAX, 10_000);
        udp_state.set_v1_rollout(DeterministicProtocolRolloutStage::AlwaysV1);

        let group_map: std::collections::BTreeMap<_, _> = std::collections::BTreeMap::new();
        let proposer_map = StubProposerSchedule::default();
        let mut decoded = Vec::new();
        for packet in packets_a.iter().chain(packets_b.iter()) {
            let recv_msg = AuthRecvMsg {
                src_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8000),
                payload: packet.payload.clone(),
                stride: packet.stride as u16,
                sender: None,
            };
            decoded.extend(udp_state.handle_message(
                &group_map,
                &fn_group_map,
                &proposer_map,
                |_, _, _| {},
                recv_msg,
            ));
        }

        // Only one of the two broadcasts should decode, whichever the
        // receiver committed to first.
        assert_eq!(decoded.len(), 1);
        let (_, msg) = &decoded[0];
        assert!(*msg == app_a || *msg == app_b);
    }
}
