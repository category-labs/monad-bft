use std::{cell::RefCell, collections::BTreeSet, ops::Range, rc::Rc, sync::Arc};

use bytes::Bytes;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor::ExecutorMetricsChain;
use monad_types::{Epoch, NodeId, Round, RouterTarget};
use monad_validator::validator_set::ValidatorSet;
use tracing::{debug, warn};

use crate::{
    decoding::DecoderCache,
    metrics::UdpStateMetrics,
    protocol::{
        parser::{RaptorcastPacket, RaptorcastPacketVersioned, SignedOverData},
        point_to_point::PointToPointState,
        primary_raptorcast::PrimaryRaptorcastState,
        secondary_raptorcast::{SecondaryRaptorcastClientState, SecondaryRaptorcastPublisherState},
        signature_verifier::SignatureVerifier,
        tcp_point_to_point::TcpPointToPointState,
    },
    util::{AuthenticatedPayload, BroadcastMode, Collector, TcpMessage, UdpMessage},
};

pub mod signature_verifier;

pub type SharedDecoderCache<PT> = Rc<RefCell<DecoderCache<PT>>>;
pub type SharedProtocolMetrics = Rc<RefCell<UdpStateMetrics>>;
pub type ChunkSignatureVerifier<ST> =
    SignatureVerifier<ST, SignedOverData, monad_crypto::signing_domain::RaptorcastChunk>;
pub type SharedChunkSignatureVerifier<ST> = Rc<RefCell<ChunkSignatureVerifier<ST>>>;

mod parser;
pub mod point_to_point;
pub mod primary_raptorcast;
pub mod secondary_raptorcast;
pub mod tcp_point_to_point;

#[derive(Debug)]
pub enum PublishError {
    NotReady,    // protocol not ready (e.g. validator set not available for epoch)
    Unsupported, // protocol not supported (e.g. publish primary on a full node)
    Build(crate::packet::BuildError),
}

#[derive(Debug)]
pub enum ReceiveError {
    NotReady,    // protocol not ready (e.g. validator set not available for epoch)
    Unsupported, // protocol not supported (e.g. receive primary raptorcast chunk on a full node)
    UdpMessageValidation(crate::udp::MessageValidationError),
    TcpMessageValidation(tcp_point_to_point::TcpValidationError),
    Decoding(crate::decoding::TryDecodeError),
    Loopback,     // Received a message with self as author
    NotInGroup,   // Self is not in the validator set/secondary group
    NotRecipient, // Self is not the intended recipient (point-to-point)
    RateLimited,  // Signature verification rate limited
}

#[allow(dead_code)]
pub(crate) trait PublishResultLogging {
    fn log_publish_error(self, context: &str) -> Self;
}

impl PublishResultLogging for Result<(), PublishError> {
    fn log_publish_error(self, context: &str) -> Self {
        let Err(ref e) = self else {
            return self;
        };
        match e {
            PublishError::NotReady => {
                debug!(context, "publish failed: not ready");
            }
            PublishError::Unsupported => {
                debug!(context, "publish failed: unsupported");
            }
            PublishError::Build(err) => {
                warn!(context, ?err, "publish failed: build error");
            }
        }
        self
    }
}

#[allow(dead_code)]
pub(crate) trait ReceiveResultLogging {
    fn log_receive_error(self, context: &str) -> Self;
}

impl ReceiveResultLogging for Result<(), ReceiveError> {
    fn log_receive_error(self, context: &str) -> Self {
        let Err(ref e) = self else {
            return self;
        };

        match e {
            ReceiveError::NotReady => {
                debug!(context, "receive failed: not ready");
            }
            ReceiveError::Unsupported => {
                debug!(context, "receive failed: unsupported");
            }
            ReceiveError::UdpMessageValidation(err) => {
                debug!(?err, "unable to parse udp message");
            }
            ReceiveError::TcpMessageValidation(err) => {
                debug!(?err, "unable to parse tcp message");
            }
            // Decoding errors are logged by DecodingResultLogging
            ReceiveError::Decoding(_) => {}
            ReceiveError::Loopback => {
                debug!(context, "receive: loopback message");
            }
            ReceiveError::NotInGroup => {
                debug!(context, "receive failed: author not in group");
            }
            ReceiveError::NotRecipient => {
                debug!(context, "receive: not intended recipient");
            }
            ReceiveError::RateLimited => {
                debug!(context, "receive failed: rate limited");
            }
        }
        self
    }
}

// Dispatcher is meant to be a very thin wrapper around the protocol
// states with the goal of eventually merging the individual protocols
// into the Raptorcast or RaptorcastSecondary::Role.
pub struct Dispatcher<ST: CertificateSignatureRecoverable> {
    // Required for all nodes (even full nodes can receive primary raptorcast chunks?)
    primary: PrimaryRaptorcastState<ST>,
    // Required for validator node who participates in secondary publishing
    secondary_publisher: Option<SecondaryRaptorcastPublisherState<ST>>,
    // Required for full nodes
    secondary_client: Option<SecondaryRaptorcastClientState<ST>>,
    // Required for all nodes
    point_to_point: PointToPointState<ST>,
    // Required for all nodes
    tcp_point_to_point: TcpPointToPointState<ST>,

    // just for fetching metrics
    decoder_cache: Option<SharedDecoderCache<CertificateSignaturePubKey<ST>>>,
    shared_udp_metrics: Option<SharedProtocolMetrics>,
}

impl<ST> Dispatcher<ST>
where
    ST: CertificateSignatureRecoverable,
{
    pub fn new(
        primary: PrimaryRaptorcastState<ST>,
        secondary_publisher: Option<SecondaryRaptorcastPublisherState<ST>>,
        secondary_client: Option<SecondaryRaptorcastClientState<ST>>,
        point_to_point: PointToPointState<ST>,
        tcp_point_to_point: TcpPointToPointState<ST>,
    ) -> Self {
        Self {
            primary,
            secondary_publisher,
            secondary_client,
            point_to_point,
            tcp_point_to_point,

            decoder_cache: None,
            shared_udp_metrics: None,
        }
    }

    pub fn with_signature_verifier(mut self, verifier: ChunkSignatureVerifier<ST>) -> Self {
        let shared = Rc::new(RefCell::new(verifier));
        self.primary.set_signature_verifier(shared.clone());
        if let Some(ref mut secondary_client) = self.secondary_client {
            secondary_client.set_signature_verifier(shared.clone());
        }
        self.point_to_point.set_signature_verifier(shared);
        self
    }

    pub fn with_decoder_cache(
        mut self,
        decoder_cache: DecoderCache<CertificateSignaturePubKey<ST>>,
    ) -> Self {
        let shared = Rc::new(RefCell::new(decoder_cache));
        self.primary.set_decoder_cache(shared.clone());
        if let Some(ref mut secondary_client) = self.secondary_client {
            secondary_client.set_decoder_cache(shared.clone());
        }
        self.point_to_point.set_decoder_cache(shared.clone());
        self.decoder_cache = Some(shared);
        self
    }

    pub fn with_metrics(mut self) -> Self {
        let shared = Rc::new(RefCell::new(UdpStateMetrics::new()));
        self.primary.set_metrics(shared.clone());
        if let Some(ref mut secondary_client) = self.secondary_client {
            secondary_client.set_metrics(shared.clone());
        }
        self.shared_udp_metrics = Some(shared);
        self
    }

    pub fn metrics(&self) -> ExecutorMetricsChain<'_> {
        // TODO: figure out how to extract the metrics out of the RefCells.
        ExecutorMetricsChain::default()
    }

    pub fn enter_round(&mut self, curr_epoch: Epoch, curr_round: Round) {
        self.primary.prune_group(curr_epoch);
        self.point_to_point.enter_epoch(curr_epoch);

        if let Some(ref mut publisher) = self.secondary_publisher {
            publisher.prune_groups(curr_round);
        }

        if let Some(ref mut client) = self.secondary_client {
            client.prune_groups(curr_round);
        }
    }

    pub fn update_epoch_validators(
        &mut self,
        epoch: Epoch,
        validator_set: Arc<ValidatorSet<CertificateSignaturePubKey<ST>>>,
    ) {
        self.primary.update_group(epoch, validator_set.clone());
        // Update point-to-point state (for Target::Validators broadcasts)
        self.point_to_point.update_group(epoch, validator_set);
    }

    // The caller should ensure the `nodes` set doesn't include self
    // for secondary client.
    pub fn update_secondary_group(
        &mut self,
        publisher_id: NodeId<CertificateSignaturePubKey<ST>>,
        round_span: Range<Round>,
        nodes: BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>,
    ) {
        if let Some(ref mut publisher) = self.secondary_publisher {
            debug_assert_eq!(publisher.self_id(), &publisher_id);
            publisher.update_group(round_span.clone(), nodes.clone());
        }

        if let Some(ref mut client) = self.secondary_client {
            client.update_group(publisher_id, round_span, nodes);
        }
    }

    pub fn publish_tcp(
        &mut self,
        to: NodeId<CertificateSignaturePubKey<ST>>,
        completion: Option<futures::channel::oneshot::Sender<()>>,
        data: Bytes,
        tcp_packet_collector: &mut impl Collector<TcpMessage<CertificateSignaturePubKey<ST>>>,
        data_collector: &mut impl Collector<AuthenticatedPayload<CertificateSignaturePubKey<ST>>>,
    ) -> Result<(), PublishError> {
        self.tcp_point_to_point
            .publish(data, to, completion, tcp_packet_collector, data_collector)
            .map_err(|e| match e {/* infallible */})
    }

    pub fn publish_primary(
        &mut self,
        epoch: Epoch,
        data: Bytes,
        udp_packet_collector: &mut impl Collector<UdpMessage<CertificateSignaturePubKey<ST>>>,
        data_collector: &mut impl Collector<AuthenticatedPayload<CertificateSignaturePubKey<ST>>>,
    ) -> Result<(), PublishError> {
        self.primary
            .publish(data, epoch, udp_packet_collector, data_collector)
    }

    pub fn publish_secondary(
        &mut self,
        round: Round,
        data: Bytes,
        udp_packet_collector: &mut impl Collector<UdpMessage<CertificateSignaturePubKey<ST>>>,
    ) -> Result<(), PublishError> {
        self.secondary_publisher
            .as_mut()
            .ok_or(PublishError::Unsupported)?
            .publish(data, round, udp_packet_collector)
            .map_err(PublishError::Build)
    }

    pub fn publish_point_to_point(
        &mut self,
        target: point_to_point::PointToPointTarget<CertificateSignaturePubKey<ST>>,
        data: Bytes,
        udp_packet_collector: &mut impl Collector<UdpMessage<CertificateSignaturePubKey<ST>>>,
        data_collector: &mut impl Collector<AuthenticatedPayload<CertificateSignaturePubKey<ST>>>,
    ) -> Result<(), PublishError> {
        self.point_to_point
            .publish(data, target, udp_packet_collector, data_collector)
    }

    pub fn publish(
        &mut self,
        target: RouterTarget<CertificateSignaturePubKey<ST>>,
        data: Bytes,
        udp_packet_collector: &mut impl Collector<UdpMessage<CertificateSignaturePubKey<ST>>>,
        tcp_packet_collector: &mut impl Collector<TcpMessage<CertificateSignaturePubKey<ST>>>,
        data_collector: &mut impl Collector<AuthenticatedPayload<CertificateSignaturePubKey<ST>>>,
    ) -> Result<(), PublishError> {
        match target {
            RouterTarget::Broadcast(epoch) => self.publish_point_to_point(
                point_to_point::PointToPointTarget::Validators(epoch),
                data,
                udp_packet_collector,
                data_collector,
            ),
            RouterTarget::Raptorcast(epoch) => {
                self.publish_primary(epoch, data, udp_packet_collector, data_collector)
            }
            RouterTarget::PointToPoint(node_id) => self.publish_point_to_point(
                point_to_point::PointToPointTarget::Single(node_id),
                data,
                udp_packet_collector,
                data_collector,
            ),
            RouterTarget::TcpPointToPoint { to, completion } => {
                self.publish_tcp(to, completion, data, tcp_packet_collector, data_collector)
            }
        }
    }

    pub fn receive_tcp(
        &mut self,
        _auth_sender: Option<NodeId<CertificateSignaturePubKey<ST>>>,
        message: Bytes,
        disconnect: impl FnOnce(),
        data_collector: &mut impl Collector<AuthenticatedPayload<CertificateSignaturePubKey<ST>>>,
    ) -> Result<(), ReceiveError> {
        match self.tcp_point_to_point.receive(message, data_collector) {
            Ok(()) => Ok(()),
            Err(e) => {
                disconnect();
                Err(e)
            }
        }
    }

    pub fn receive_udp(
        &mut self,
        auth_sender: Option<NodeId<CertificateSignaturePubKey<ST>>>,
        packet_bytes: Bytes,
        packet_collector: &mut impl Collector<UdpMessage<CertificateSignaturePubKey<ST>>>,
        data_collector: &mut impl Collector<AuthenticatedPayload<CertificateSignaturePubKey<ST>>>,
    ) -> Result<(), ReceiveError> {
        let RaptorcastPacket {
            common_header,
            versioned: RaptorcastPacketVersioned::V0(packet_v0),
        } = parser::RaptorcastPacket::parse(&packet_bytes)
            .map_err(ReceiveError::UdpMessageValidation)?;

        let broadcast_mode = packet_v0
            .header
            .broadcast_mode()
            .map_err(ReceiveError::UdpMessageValidation)?;

        match broadcast_mode {
            BroadcastMode::Primary => self.primary.receive(
                auth_sender,
                &common_header,
                &packet_v0,
                &packet_bytes,
                packet_collector,
                data_collector,
            ),
            BroadcastMode::Secondary => self
                .secondary_client
                .as_mut()
                .ok_or(ReceiveError::Unsupported)?
                .receive(
                    auth_sender,
                    &common_header,
                    &packet_v0,
                    &packet_bytes,
                    packet_collector,
                    data_collector,
                ),
            BroadcastMode::Unspecified => self.point_to_point.receive(
                auth_sender,
                &common_header,
                &packet_v0,
                &packet_bytes,
                data_collector,
            ),
        }
    }

    // Leak the validator set. Ideally we should not expose the
    // validator set outside the protocol layers, but currently there
    // are places where we need to use it:
    //
    // - Validation of FullNodesGroupMessage
    // - Updated the trusted TCP peers in UpdateCurrentRound
    pub fn get_validator_set(
        &self,
        epoch: Epoch,
    ) -> Option<&ValidatorSet<CertificateSignaturePubKey<ST>>> {
        self.primary.get_validator_set(epoch)
    }
}
