use std::{
    cell::RefCell,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_dataplane::{udp::ETHERNET_SEGMENT_SIZE, UnicastMsg};
use monad_peer_discovery::{driver::PeerDiscoveryDriver, NameRecord, PeerDiscoveryAlgo};
use monad_types::{NodeId, UdpPriority};
use tracing::warn;

use crate::{
    auth::{AuthenticationProtocol, DualSocketHandle},
    packet::{PeerAddrLookup, RetrofitResult as _, UdpMessageBatcher},
    util::BuildTarget,
    OwnedMessageBuilder, DEFAULT_RETRY_ATTEMPTS, UNICAST_MSG_BATCH_SIZE,
};

pub struct Sender<'a, ST, PD, AP>
where
    ST: CertificateSignatureRecoverable,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
    AP: AuthenticationProtocol<PublicKey = CertificateSignaturePubKey<ST>>,
{
    dual_socket: &'a mut DualSocketHandle<AP>,
    peer_discovery_driver: &'a Arc<Mutex<PeerDiscoveryDriver<PD>>>,
    message_builder: &'a mut OwnedMessageBuilder<ST, PD>,
}

impl<'a, ST, PD, AP> Sender<'a, ST, PD, AP>
where
    ST: CertificateSignatureRecoverable,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
    AP: AuthenticationProtocol<PublicKey = CertificateSignaturePubKey<ST>>,
{
    pub fn new(
        dual_socket: &'a mut DualSocketHandle<AP>,
        peer_discovery_driver: &'a Arc<Mutex<PeerDiscoveryDriver<PD>>>,
        message_builder: &'a mut OwnedMessageBuilder<ST, PD>,
    ) -> Self {
        Self {
            dual_socket,
            peer_discovery_driver,
            message_builder,
        }
    }

    pub fn send(
        &mut self,
        message: &Bytes,
        build_target: &BuildTarget<ST>,
        priority: UdpPriority,
        epoch: monad_types::Epoch,
    ) {
        {
            let dual_socket_cell = RefCell::new(&mut *self.dual_socket);
            let mut sink = UdpMessageBatcher::new(UNICAST_MSG_BATCH_SIZE, |rc_chunks| {
                dual_socket_cell
                    .borrow_mut()
                    .write_unicast_with_priority(rc_chunks, priority);
            });

            self.message_builder
                .prepare_with_peer_lookup((self.peer_discovery_driver, &dual_socket_cell))
                .epoch_no(epoch)
                .build_into(message, build_target, &mut sink)
                .unwrap_log_on_error(message, build_target);
        }

        // for 200 validators establishing sessions at once can consume up to 20ms of time, e.g 100us per validator
        // to avoid problems if first message is sent right after restart we try to initiate sessions after sending out payloads
        ensure_authenticated_sessions(
            self.dual_socket,
            self.peer_discovery_driver,
            build_target.iter(),
        );
    }

    pub fn send_with_record(
        &mut self,
        message: &Bytes,
        priority: UdpPriority,
        target: &NodeId<CertificateSignaturePubKey<ST>>,
        name_record: &NameRecord,
    ) {
        let build_target: BuildTarget<'_, ST> = BuildTarget::PointToPoint(target);
        let should_authenticate = name_record.authenticated_udp_socket().is_some();

        {
            let dual_socket_cell = RefCell::new(&mut *self.dual_socket);
            let lookup = NameRecordLookup::<ST, AP> {
                target: *target,
                name_record,
                dual_socket: &dual_socket_cell,
            };
            let mut sink = UdpMessageBatcher::new(UNICAST_MSG_BATCH_SIZE, |rc_chunks| {
                dual_socket_cell
                    .borrow_mut()
                    .write_unicast_with_priority(rc_chunks, priority);
            });

            self.message_builder
                .prepare_with_peer_lookup(&lookup)
                .build_into(message, &build_target, &mut sink)
                .unwrap_log_on_error(message, &build_target);
        }

        if should_authenticate {
            ensure_authenticated_sessions(
                self.dual_socket,
                self.peer_discovery_driver,
                std::iter::once(target),
            );
        }
    }

    pub fn rebroadcast_packet(
        &mut self,
        target: &NodeId<CertificateSignaturePubKey<ST>>,
        payload: Bytes,
        bcast_stride: u16,
    ) {
        let can_authenticate =
            payload.len() + AP::HEADER_SIZE as usize <= ETHERNET_SEGMENT_SIZE as usize;

        let target_addr = self
            .dual_socket
            .get_socket_by_public_key(&target.pubkey())
            .or_else(|| {
                self.peer_discovery_driver
                    .lock()
                    .ok()
                    .and_then(|pd| pd.get_addr(target))
            });

        let Some(target_addr) = target_addr else {
            warn!(target=?target, "failed to find address for rebroadcast target");
            return;
        };

        self.dual_socket.write_unicast_with_priority(
            UnicastMsg {
                msgs: vec![(target_addr, payload)],
                stride: bcast_stride,
            },
            UdpPriority::High,
        );

        if can_authenticate {
            ensure_authenticated_sessions(
                self.dual_socket,
                self.peer_discovery_driver,
                std::iter::once(target),
            );
        }
    }
}

fn ensure_authenticated_sessions<'a, ST, PD, AP>(
    dual_socket: &mut DualSocketHandle<AP>,
    peer_discovery_driver: &Arc<Mutex<PeerDiscoveryDriver<PD>>>,
    targets: impl Iterator<Item = &'a NodeId<CertificateSignaturePubKey<ST>>>,
) where
    ST: CertificateSignatureRecoverable,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
    AP: AuthenticationProtocol<PublicKey = CertificateSignaturePubKey<ST>>,
{
    let pd_driver = peer_discovery_driver.lock().unwrap();

    targets
        .filter_map(|target| {
            pd_driver
                .get_name_record(target)
                .and_then(|record| record.name_record.authenticated_udp_socket())
                .map(|addr| (target, addr))
        })
        .for_each(|(target, auth_addr)| {
            if dual_socket.has_any_session_by_public_key(&target.pubkey()) {
                return;
            }

            if let Err(e) = dual_socket.connect(
                &target.pubkey(),
                SocketAddr::V4(auth_addr),
                DEFAULT_RETRY_ATTEMPTS,
            ) {
                warn!(
                    target=?target,
                    auth_addr=?auth_addr,
                    error=?e,
                    "failed to initiate connection to authenticated endpoint"
                );
            }
        });

    dual_socket.flush();
}

impl<ST, PD, AP> PeerAddrLookup<CertificateSignaturePubKey<ST>>
    for (
        &Arc<Mutex<PeerDiscoveryDriver<PD>>>,
        &RefCell<&mut DualSocketHandle<AP>>,
    )
where
    ST: CertificateSignatureRecoverable,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
    AP: AuthenticationProtocol<PublicKey = CertificateSignaturePubKey<ST>>,
{
    fn lookup(&self, node_id: &NodeId<CertificateSignaturePubKey<ST>>) -> Option<SocketAddr> {
        let (discovery, auth_socket) = self;

        if let Some(auth_addr) = auth_socket
            .borrow()
            .get_socket_by_public_key(&node_id.pubkey())
        {
            return Some(auth_addr);
        }

        discovery.lock().ok()?.get_addr(node_id)
    }
}

struct NameRecordLookup<'a, ST, AP>
where
    ST: CertificateSignatureRecoverable,
    AP: AuthenticationProtocol<PublicKey = CertificateSignaturePubKey<ST>>,
{
    pub target: NodeId<CertificateSignaturePubKey<ST>>,
    pub name_record: &'a NameRecord,
    pub dual_socket: &'a RefCell<&'a mut DualSocketHandle<AP>>,
}

impl<ST, AP> PeerAddrLookup<CertificateSignaturePubKey<ST>> for NameRecordLookup<'_, ST, AP>
where
    ST: CertificateSignatureRecoverable,
    AP: AuthenticationProtocol<PublicKey = CertificateSignaturePubKey<ST>>,
{
    fn lookup(&self, node_id: &NodeId<CertificateSignaturePubKey<ST>>) -> Option<SocketAddr> {
        if *node_id != self.target {
            return None;
        }

        if let Some(auth_addr) = self
            .dual_socket
            .borrow()
            .get_socket_by_public_key(&node_id.pubkey())
        {
            return Some(auth_addr);
        }

        Some(SocketAddr::V4(self.name_record.udp_socket()))
    }
}
