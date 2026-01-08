use std::sync::Arc;

use bytes::Bytes;
use monad_crypto::certificate_signature::{
    CertificateKeyPair as _, CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_types::{Epoch, NodeId};
use monad_validator::validator_set::{ValidatorSet, ValidatorSetType as _};

use super::{
    parser::{validate_chunk_id, CommonRaptorcastHeader, RaptorcastPacketV0},
    primary_raptorcast::DEFAULT_MERKLE_TREE_DEPTH,
    AuthenticatedPayload, PublishError, SharedChunkSignatureVerifier, SharedDecoderCache,
};
use crate::{
    decoding::{DecodingContext, DecodingResultLogging as _, TryDecodeStatus},
    packet::{self, assigner::Replicated},
    protocol::{parser::validate_message_v0, ReceiveError},
    udp::{GroupId, MAX_REDUNDANCY},
    util::{
        unix_ts_ms_now, BroadcastMode, Collector, EpochValidatorMap, NodeIdHash, Redundancy,
        UdpMessage,
    },
};

// For broadcast and unicast.
pub struct PointToPointState<ST>
where
    ST: CertificateSignatureRecoverable,
{
    self_id: NodeId<CertificateSignaturePubKey<ST>>,
    self_hash: NodeIdHash,
    group_map: EpochValidatorMap<CertificateSignaturePubKey<ST>>,

    builder: packet::MessageBuilder<'static, ST>,
    decoder_cache: Option<SharedDecoderCache<CertificateSignaturePubKey<ST>>>,
    signature_verifier: Option<SharedChunkSignatureVerifier<ST>>,
    max_age_ms: u64,

    // Used for compatibility with old behavior for
    // non-validator-targeted unicast messages where the group id is
    // Primary(current_epoch).
    //
    // The group id is not used except for sender validation of
    // secondary group messages.
    current_epoch: Epoch,
}

pub enum PointToPointTarget<PT: PubKey> {
    Validators(Epoch),
    Single(NodeId<PT>),
    Multi(Vec<NodeId<PT>>),
}

impl<ST> PointToPointState<ST>
where
    ST: CertificateSignatureRecoverable,
{
    pub fn new(
        keypair: Arc<ST::KeyPairType>,
        redundancy: f32,
        max_age_ms: u64,
        segment_size: usize,
    ) -> Self {
        let self_id = NodeId::new(keypair.pubkey());
        let self_hash = crate::util::compute_hash(&self_id);

        let redundancy = Redundancy::from_f32(redundancy).expect("invalid redundancy value");
        let builder = packet::MessageBuilder::new(keypair)
            .redundancy(redundancy)
            .merkle_tree_depth(DEFAULT_MERKLE_TREE_DEPTH)
            .segment_size(segment_size);
        let group_map = Default::default();

        Self {
            self_id,
            self_hash,
            builder,
            decoder_cache: None,
            signature_verifier: None,
            max_age_ms,
            group_map,
            current_epoch: Epoch(0),
        }
    }

    pub fn set_signature_verifier(&mut self, verifier: SharedChunkSignatureVerifier<ST>) {
        self.signature_verifier = Some(verifier);
    }

    pub fn set_decoder_cache(&mut self, cache: SharedDecoderCache<CertificateSignaturePubKey<ST>>) {
        self.decoder_cache = Some(cache);
    }

    pub fn enter_epoch(&mut self, curr_epoch: Epoch) {
        self.current_epoch = curr_epoch;
        self.group_map.prune(curr_epoch);
    }

    pub fn update_group(
        &mut self,
        epoch: Epoch,
        validator_set: Arc<ValidatorSet<CertificateSignaturePubKey<ST>>>,
    ) {
        self.group_map.update(epoch, validator_set);
    }

    pub fn publish(
        &mut self,
        data: Bytes,
        target: PointToPointTarget<CertificateSignaturePubKey<ST>>,
        packet_collector: &mut impl Collector<UdpMessage<CertificateSignaturePubKey<ST>>>,
        // for loop-back messages to self
        data_collector: &mut impl Collector<AuthenticatedPayload<CertificateSignaturePubKey<ST>>>,
    ) -> Result<(), PublishError> {
        let loop_back;
        let group_id;
        let target_nodes;

        match target {
            PointToPointTarget::Validators(epoch) => {
                let valset = self.group_map.get(&epoch).ok_or(PublishError::NotReady)?;
                if valset.is_member(&self.self_id) {
                    loop_back = true;
                } else {
                    // broadcasting to validators as a non-validator.
                    return Err(PublishError::Unsupported);
                }
                group_id = GroupId::Primary(epoch);
                target_nodes = valset
                    .get_members()
                    .keys()
                    .filter(|node_id| *node_id != &self.self_id)
                    .cloned()
                    .collect();
            }

            PointToPointTarget::Single(target) => {
                loop_back = target == self.self_id;
                // TODO: switch to a variant for unspecified group id
                group_id = GroupId::Primary(self.current_epoch);
                target_nodes = if loop_back { vec![] } else { vec![target] };
            }

            PointToPointTarget::Multi(mut targets) => {
                loop_back = targets.contains(&self.self_id);
                targets.retain(|node_id| node_id != &self.self_id);
                target_nodes = targets;
                group_id = GroupId::Primary(self.current_epoch);
            }
        }

        if !target_nodes.is_empty() {
            let assigner = Replicated::from_broadcast(target_nodes);

            let () = self
                .builder
                .prepare()
                .group_id(group_id)
                .broadcast_mode(BroadcastMode::Unspecified)
                .build_for(&data, assigner, packet_collector)
                .map_err(PublishError::Build)?;
        }

        if loop_back {
            data_collector.push((self.self_id, data).into());
        }

        Ok(())
    }

    pub fn receive(
        &mut self,
        auth_sender: Option<NodeId<CertificateSignaturePubKey<ST>>>,
        common_header: &CommonRaptorcastHeader<'_>,
        packet: &RaptorcastPacketV0<'_>,
        packet_bytes: &Bytes,
        data_collector: &mut impl Collector<AuthenticatedPayload<CertificateSignaturePubKey<ST>>>,
    ) -> Result<(), ReceiveError> {
        // pre-conditions that must be enforced by caller
        debug_assert!(matches!(
            packet.header.broadcast_mode(),
            Ok(BroadcastMode::Unspecified)
        ));

        // chunk validation
        validate_chunk_id(packet, MAX_REDUNDANCY, 0).map_err(ReceiveError::UdpMessageValidation)?;

        let verifier = self
            .signature_verifier
            .as_ref()
            .ok_or(ReceiveError::NotReady)?;
        let decoder_cache = self.decoder_cache.as_ref().ok_or(ReceiveError::NotReady)?;

        let bypass_rate_limiter = auth_sender.is_some_and(|sender| {
            // TODO: also check for epoch in message group id? or also
            // check for the next epoch?
            self.group_map
                .get(&self.current_epoch)
                .is_some_and(|valset| valset.is_member(&sender))
        });

        let chunk = validate_message_v0::<ST>(
            &mut verifier.borrow_mut(),
            common_header,
            packet,
            packet_bytes,
            self.max_age_ms,
            bypass_rate_limiter,
        )
        .map_err(ReceiveError::UdpMessageValidation)?;

        // chunk author/recipient validation
        if chunk.author == self.self_id {
            return Err(ReceiveError::Loopback);
        }
        if chunk.recipient_hash != self.self_hash {
            return Err(ReceiveError::NotRecipient);
        }

        // chunk decoding
        let decoding_context = DecodingContext::new(None, unix_ts_ms_now());
        let decode_result = decoder_cache
            .borrow_mut()
            .try_decode(&chunk, &decoding_context)
            .log_decoding_error(&chunk, "point-to-point rc")
            .map_err(ReceiveError::Decoding)?;

        // return decoded app message
        match decode_result {
            TryDecodeStatus::RejectedByCache
            | TryDecodeStatus::RecentlyDecoded
            | TryDecodeStatus::NeedsMoreSymbols => {}
            TryDecodeStatus::Decoded { app_message, .. } => {
                data_collector.push((chunk.author, app_message).into());
            }
        }

        Ok(())
    }
}
