use std::sync::Arc;

use bytes::Bytes;
use monad_crypto::certificate_signature::{
    CertificateKeyPair as _, CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_types::{Epoch, NodeId};
use monad_validator::validator_set::{ValidatorSet, ValidatorSetType};

use super::{
    parser::{CommonRaptorcastHeader, RaptorcastPacketV0},
    PublishError, SharedChunkSignatureVerifier, SharedDecoderCache, SharedProtocolMetrics,
};
use crate::{
    decoding::{DecodingContext, DecodingResultLogging as _, TryDecodeStatus},
    packet::{self, assigner::StakeBasedWithRC},
    protocol::{
        parser::{validate_chunk_id, validate_message_v0},
        ReceiveError,
    },
    udp::{GroupId, MAX_REDUNDANCY},
    util::{
        compute_app_message_hash, unix_ts_ms_now, AuthenticatedPayload, BroadcastMode, Collector,
        EpochValidatorMap, NodeIdHash, Recipient, Redundancy, UdpMessage,
    },
};

pub struct PrimaryRaptorcastState<ST: CertificateSignatureRecoverable> {
    self_id: NodeId<CertificateSignaturePubKey<ST>>,
    self_hash: NodeIdHash,

    group_map: EpochValidatorMap<CertificateSignaturePubKey<ST>>,
    builder: packet::MessageBuilder<'static, ST>,

    decoder_cache: Option<SharedDecoderCache<CertificateSignaturePubKey<ST>>>,
    signature_verifier: Option<SharedChunkSignatureVerifier<ST>>,
    metrics: Option<SharedProtocolMetrics>,
    max_age_ms: u64,
}

pub const DEFAULT_MERKLE_TREE_DEPTH: u8 = 6;

impl<ST> PrimaryRaptorcastState<ST>
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

        Self {
            self_id,
            self_hash,
            group_map: Default::default(),
            decoder_cache: None,
            builder,
            signature_verifier: None,
            metrics: None,
            max_age_ms,
        }
    }

    pub fn set_signature_verifier(&mut self, verifier: SharedChunkSignatureVerifier<ST>) {
        self.signature_verifier = Some(verifier);
    }

    pub fn set_decoder_cache(&mut self, cache: SharedDecoderCache<CertificateSignaturePubKey<ST>>) {
        self.decoder_cache = Some(cache);
    }

    pub fn set_metrics(&mut self, metrics: SharedProtocolMetrics) {
        self.metrics = Some(metrics);
    }

    pub fn get_validator_set(
        &self,
        epoch: Epoch,
    ) -> Option<&ValidatorSet<CertificateSignaturePubKey<ST>>> {
        self.group_map.get(&epoch)
    }

    pub fn prune_group(&mut self, curr_epoch: Epoch) {
        self.group_map.prune(curr_epoch);
    }

    // The caller must ensure self_id is in the validator_set
    pub fn update_group(
        &mut self,
        epoch: Epoch,
        validator_set: Arc<ValidatorSet<CertificateSignaturePubKey<ST>>>,
    ) {
        debug_assert!(validator_set.is_member(&self.self_id));
        self.group_map.update(epoch, validator_set);
    }

    pub fn publish(
        &mut self,
        data: Bytes,
        epoch: Epoch,
        packet_collector: &mut impl Collector<UdpMessage<CertificateSignaturePubKey<ST>>>,
        // for loop-back messages to self
        data_collector: &mut impl Collector<AuthenticatedPayload<CertificateSignaturePubKey<ST>>>,
    ) -> Result<(), PublishError> {
        let validator_set = self.group_map.get(&epoch).ok_or(PublishError::NotReady)?;
        if !validator_set.is_member(&self.self_id) {
            // cannot publish primary raptorcast if self is not in the
            // validator set.
            return Err(PublishError::Unsupported);
        }

        let app_message_hash = compute_app_message_hash(&data).0;
        let seed = StakeBasedWithRC::<CertificateSignaturePubKey<ST>>::seed_from_app_message_hash(
            &app_message_hash,
        );
        let validator_set =
            StakeBasedWithRC::shuffle_validators(validator_set, Some(&self.self_id), seed);
        let assigner = StakeBasedWithRC::from_validator_set(validator_set);

        let () = self
            .builder
            .prepare()
            .group_id(GroupId::Primary(epoch))
            .app_message_hash(app_message_hash)
            .broadcast_mode(BroadcastMode::Primary)
            .build_for(&data, assigner, packet_collector)
            .map_err(PublishError::Build)?;

        // loopback message to self
        data_collector.push((self.self_id, data).into());

        Ok(())
    }

    pub fn receive(
        &mut self,
        auth_sender: Option<NodeId<CertificateSignaturePubKey<ST>>>,
        common_header: &CommonRaptorcastHeader<'_>,
        packet: &RaptorcastPacketV0<'_>,
        packet_bytes: &Bytes,
        packet_collector: &mut impl Collector<UdpMessage<CertificateSignaturePubKey<ST>>>,
        data_collector: &mut impl Collector<AuthenticatedPayload<CertificateSignaturePubKey<ST>>>,
    ) -> Result<(), ReceiveError> {
        // pre-conditions that must be enforced by caller
        let GroupId::Primary(epoch) = packet
            .header
            .group_id()
            .map_err(ReceiveError::UdpMessageValidation)?
        else {
            panic!("PrimaryRaptorcastState should only handle primary group message");
        };
        debug_assert!(matches!(
            packet.header.broadcast_mode(),
            Ok(BroadcastMode::Primary)
        ));

        // chunk validation
        let validator_set = self.group_map.get(&epoch).ok_or(ReceiveError::NotReady)?;
        let max_rounding_chunks = validator_set.len();
        validate_chunk_id(packet, MAX_REDUNDANCY, max_rounding_chunks)
            .map_err(ReceiveError::UdpMessageValidation)?;

        let verifier = self
            .signature_verifier
            .as_ref()
            .ok_or(ReceiveError::NotReady)?;
        let decoder_cache = self.decoder_cache.as_ref().ok_or(ReceiveError::NotReady)?;

        let bypass_rate_limiter = auth_sender
            .as_ref()
            .map(|sender| validator_set.is_member(sender))
            .unwrap_or(false);
        let chunk = validate_message_v0::<ST>(
            &mut verifier.borrow_mut(),
            common_header,
            packet,
            packet_bytes,
            self.max_age_ms,
            bypass_rate_limiter,
        )
        .map_err(ReceiveError::UdpMessageValidation)?;

        // chunk author validation
        if chunk.author == self.self_id {
            return Err(ReceiveError::Loopback);
        }
        if !validator_set.is_member(&chunk.author) {
            return Err(ReceiveError::NotInGroup);
        }

        // chunk decoding
        let context = DecodingContext::new(Some(validator_set), unix_ts_ms_now());
        let decode_result = decoder_cache
            .borrow_mut()
            .try_decode(&chunk, &context)
            .log_decoding_error(&chunk, "primary rc")
            .map_err(ReceiveError::Decoding)?;

        let app_message = match decode_result {
            TryDecodeStatus::RejectedByCache => return Ok(()),
            TryDecodeStatus::RecentlyDecoded | TryDecodeStatus::NeedsMoreSymbols => None,
            TryDecodeStatus::Decoded { app_message, .. } => {
                if let Some(metrics) = &self.metrics {
                    metrics
                        .borrow_mut()
                        .record_broadcast_latency(BroadcastMode::Primary, chunk.unix_ts_ms);
                }
                Some(app_message)
            }
        };

        // rebroadcast if responsible
        if chunk.recipient_hash == self.self_hash {
            rebroadcast(
                validator_set,
                &self.self_id,
                &chunk.author,
                packet_bytes,
                packet_collector,
            );
        };

        // return decoded app message
        if let Some(app_message) = app_message {
            data_collector.push((chunk.author, app_message).into());
        }

        Ok(())
    }
}

fn rebroadcast<PT: PubKey>(
    validator_set: &ValidatorSet<PT>,
    self_id: &NodeId<PT>,
    author: &NodeId<PT>,
    packet: &Bytes,
    packet_collector: &mut impl Collector<UdpMessage<PT>>,
) {
    for validator in validator_set.get_members().keys() {
        if validator == self_id || validator == author {
            continue;
        }

        let rebroadcasted_message = UdpMessage {
            recipient: Recipient::new(*validator),
            payload: packet.clone(),
            stride: packet.len(),
        };
        packet_collector.push(rebroadcasted_message);
    }
}
