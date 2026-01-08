use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Range,
    sync::Arc,
};

use bytes::Bytes;
use iset::IntervalMap;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_types::{NodeId, Round};

pub use self::{
    client::{Error as ClientError, SecondaryRaptorcastClientState},
    publisher::SecondaryRaptorcastPublisherState,
};
use crate::{
    decoding::DecodingContext,
    packet::{self},
    udp::GroupId,
    util::{BroadcastMode, Collector, NodeIdHash, Redundancy},
};

pub const DEFAULT_MERKLE_TREE_DEPTH: u8 = 6;

// An non-overlapping interval map from RoundSpan to Group
pub struct GroupMap<Group>(IntervalMap<Round, Group>);

impl<Group> Default for GroupMap<Group> {
    fn default() -> Self {
        Self(IntervalMap::new())
    }
}

impl<Group> GroupMap<Group> {
    fn get(&self, round: Round) -> Option<&Group> {
        let mut iter = self.0.overlap(round);
        let (_range, v) = iter.next()?;
        Some(v)
    }

    // round_cap is exclusive.
    fn prune(&mut self, round_cap: Round) {
        while let Some((range, _)) = self.0.smallest() {
            if range.end <= round_cap {
                self.0.remove(range);
            } else {
                break;
            }
        }
    }

    fn insert(&mut self, round_span: Range<Round>, group: Group) {
        debug_assert!(!self.0.has_overlap(round_span.clone()));
        self.0.insert(round_span, group);
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

pub mod publisher {
    use monad_crypto::certificate_signature::CertificateKeyPair as _;
    use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng as _};

    use super::*;
    use crate::{
        packet::{assigner::Partitioned, BuildError},
        util::{compute_app_message_hash, UdpMessage},
    };

    // TODO: reduce hashing of node ids on build message by storing Recipient<PT>
    type Group<PT> = BTreeSet<NodeId<PT>>;

    // The state at the publisher side (the validator as manager of a group)
    pub struct SecondaryRaptorcastPublisherState<ST: CertificateSignatureRecoverable> {
        self_id: NodeId<CertificateSignaturePubKey<ST>>,
        group_map: GroupMap<Group<CertificateSignaturePubKey<ST>>>,
        builder: packet::MessageBuilder<'static, ST>,
    }

    impl<ST> SecondaryRaptorcastPublisherState<ST>
    where
        ST: CertificateSignatureRecoverable,
    {
        pub fn new(keypair: Arc<ST::KeyPairType>, redundancy: f32, segment_size: usize) -> Self {
            let self_id = NodeId::new(keypair.pubkey());
            let redundancy = Redundancy::from_f32(redundancy).expect("invalid redundancy value");
            let builder = packet::MessageBuilder::new(keypair)
                .redundancy(redundancy)
                .merkle_tree_depth(DEFAULT_MERKLE_TREE_DEPTH)
                .segment_size(segment_size);

            Self {
                self_id,
                group_map: Default::default(),
                builder,
            }
        }

        pub fn prune_groups(&mut self, curr_round: Round) {
            self.group_map.prune(curr_round);
        }

        pub fn self_id(&self) -> &NodeId<CertificateSignaturePubKey<ST>> {
            &self.self_id
        }

        pub fn update_group(
            &mut self,
            round_span: Range<Round>,
            nodes: BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>,
        ) {
            self.group_map.insert(round_span, nodes);
        }

        pub fn publish(
            &mut self,
            data: Bytes,
            round: Round,
            collector: &mut impl Collector<UdpMessage<CertificateSignaturePubKey<ST>>>,
        ) -> Result<(), BuildError> {
            let Some(group) = self.group_map.get(round) else {
                // group not setup yet
                return Ok(());
            };

            let app_message_hash = compute_app_message_hash(&data).0;
            let mut seed = [0; 32];
            seed[..20].copy_from_slice(&app_message_hash);

            let mut rng = StdRng::from_seed(seed);

            let mut nodes = group.iter().copied().collect::<Vec<_>>();
            nodes.shuffle(&mut rng);

            let assigner = Partitioned::from_homogeneous_peers(nodes);

            let () = self
                .builder
                .prepare()
                .group_id(GroupId::Secondary(round))
                .broadcast_mode(BroadcastMode::Secondary)
                .app_message_hash(app_message_hash)
                .build_for(&data, assigner, collector)?;

            Ok(())
        }
    }
}

pub mod client {
    use super::*;
    use crate::{
        decoding::{DecodingResultLogging as _, TryDecodeStatus},
        protocol::{
            parser::{
                validate_chunk_id, validate_message_v0, CommonRaptorcastHeader, RaptorcastPacketV0,
            },
            AuthenticatedPayload, ReceiveError, SharedChunkSignatureVerifier, SharedDecoderCache,
            SharedProtocolMetrics,
        },
        udp::MAX_REDUNDANCY,
        util::{unix_ts_ms_now, Recipient, UdpMessage},
    };

    pub enum Error {
        NotReady,
        MessageValidation(crate::udp::MessageValidationError),
        Decoding(crate::decoding::TryDecodeError),
    }

    type Group<PT> = BTreeSet<NodeId<PT>>;

    // The state at the client side (the full node participating in multiple groups)
    pub struct SecondaryRaptorcastClientState<ST: CertificateSignatureRecoverable> {
        self_id: NodeId<CertificateSignaturePubKey<ST>>,
        self_hash: NodeIdHash,

        // A full node may participate in multiple groups. Maps from
        // validator_node_id -> GroupMap.
        participated_groups: BTreeMap<
            // publisher (validator)
            NodeId<CertificateSignaturePubKey<ST>>,
            // groups managed by the publisher
            GroupMap<Group<CertificateSignaturePubKey<ST>>>,
        >,

        decoder_cache: Option<SharedDecoderCache<CertificateSignaturePubKey<ST>>>,
        signature_verifier: Option<SharedChunkSignatureVerifier<ST>>,
        metrics: Option<SharedProtocolMetrics>,
        max_age_ms: u64,
    }

    impl<ST: CertificateSignatureRecoverable> SecondaryRaptorcastClientState<ST> {
        pub fn new(self_id: NodeId<CertificateSignaturePubKey<ST>>, max_age_ms: u64) -> Self {
            let self_hash = crate::util::compute_hash(&self_id);

            Self {
                self_id,
                self_hash,
                participated_groups: Default::default(),
                decoder_cache: None,
                signature_verifier: None,
                metrics: None,
                max_age_ms,
            }
        }

        pub fn set_signature_verifier(&mut self, verifier: SharedChunkSignatureVerifier<ST>) {
            self.signature_verifier = Some(verifier);
        }

        pub fn set_decoder_cache(
            &mut self,
            cache: SharedDecoderCache<CertificateSignaturePubKey<ST>>,
        ) {
            self.decoder_cache = Some(cache);
        }

        pub fn set_metrics(&mut self, metrics: SharedProtocolMetrics) {
            self.metrics = Some(metrics);
        }

        pub fn prune_groups(&mut self, curr_round: Round) {
            let mut empty_publishers = vec![];

            for (validator, group_map) in self.participated_groups.iter_mut() {
                group_map.prune(curr_round);
                if group_map.is_empty() {
                    empty_publishers.push(*validator);
                }
            }

            for validator in empty_publishers {
                self.participated_groups.remove(&validator);
            }
        }

        // The caller must ensure self_id is not in the other_peers set
        pub fn update_group(
            &mut self,
            publisher_id: NodeId<CertificateSignaturePubKey<ST>>,
            round_span: Range<Round>,
            other_peers: BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>,
        ) {
            let group_map = self.participated_groups.entry(publisher_id).or_default();
            group_map.insert(round_span, other_peers);
        }

        pub fn receive(
            &mut self,
            _auth_sender: Option<NodeId<CertificateSignaturePubKey<ST>>>,
            common_header: &CommonRaptorcastHeader<'_>,
            packet: &RaptorcastPacketV0<'_>,
            packet_bytes: &Bytes,
            packet_collector: &mut impl Collector<UdpMessage<CertificateSignaturePubKey<ST>>>,
            data_collector: &mut impl Collector<AuthenticatedPayload<CertificateSignaturePubKey<ST>>>,
        ) -> Result<(), ReceiveError> {
            // pre-conditions that must be enforced by caller
            let GroupId::Secondary(round) = packet
                .header
                .group_id()
                .map_err(ReceiveError::UdpMessageValidation)?
            else {
                panic!("SecondaryRaptorcastState should only handle secondary group message");
            };
            debug_assert!(matches!(
                packet.header.broadcast_mode(),
                Ok(BroadcastMode::Secondary)
            ));

            // chunk validation
            validate_chunk_id(packet, MAX_REDUNDANCY, 0)
                .map_err(ReceiveError::UdpMessageValidation)?;

            let verifier = self
                .signature_verifier
                .as_ref()
                .ok_or(ReceiveError::NotReady)?;
            let decoder_cache = self.decoder_cache.as_ref().ok_or(ReceiveError::NotReady)?;

            // TODO: bypass rate limit based on auth_sender (prioritize publishers).
            let chunk = validate_message_v0::<ST>(
                &mut verifier.borrow_mut(),
                common_header,
                packet,
                packet_bytes,
                self.max_age_ms,
                false,
            )
            .map_err(ReceiveError::UdpMessageValidation)?;

            // chunk author validation
            // TODO: validate auth_sender (either the publisher, or a full node from the group).
            let group_maps = self
                .participated_groups
                .get(&chunk.author)
                .ok_or(ReceiveError::NotReady)?;
            let group = group_maps.get(round).ok_or(ReceiveError::NotReady)?;

            // chunk decoding
            let context = DecodingContext::new(None, unix_ts_ms_now());
            let decode_result = decoder_cache
                .borrow_mut()
                .try_decode(&chunk, &context)
                .log_decoding_error(&chunk, "secondary rc")
                .map_err(ReceiveError::Decoding)?;

            let app_message = match decode_result {
                TryDecodeStatus::RejectedByCache => return Ok(()),
                TryDecodeStatus::RecentlyDecoded | TryDecodeStatus::NeedsMoreSymbols => None,
                TryDecodeStatus::Decoded { app_message, .. } => {
                    if let Some(metrics) = &self.metrics {
                        metrics
                            .borrow_mut()
                            .record_broadcast_latency(BroadcastMode::Secondary, chunk.unix_ts_ms);
                    }
                    Some(app_message)
                }
            };

            // rebroadcast if responsible
            if chunk.recipient_hash == self.self_hash {
                rebroadcast(packet_bytes, group, packet_collector);
            };

            // return decoded app message
            if let Some(app_message) = app_message {
                data_collector.push((chunk.author, app_message).into());
            }

            Ok(())
        }
    }

    fn rebroadcast<PT: PubKey>(
        packet: &Bytes,
        group: &Group<PT>,
        packet_collector: &mut impl Collector<UdpMessage<PT>>,
    ) {
        for node in group {
            let rebroadcasted_message = UdpMessage {
                recipient: Recipient::new(*node),
                payload: packet.clone(),
                stride: packet.len(),
            };
            packet_collector.push(rebroadcasted_message);
        }
    }
}
