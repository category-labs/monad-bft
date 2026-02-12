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

use std::{hash::Hash, marker::PhantomData, net::SocketAddr};

use bytes::{BufMut, Bytes, BytesMut};
use monad_dataplane::UnicastMsg;
use monad_executor::ExecutorMetricsChain;
use monad_leanudp::{
    Config, DecodeOutcome, Decoder, Encoder, FragmentPolicy, IdentityScore, PacketHeader,
    SystemClock,
};
use monad_peer_score::PeerStatus;
use monad_types::UdpPriority;
use zerocopy::IntoBytes;

use super::{protocol::AuthenticationProtocol, socket::AuthenticatedSocketHandle};

pub struct PeerScoreAdapter<S> {
    score_reader: S,
}

impl<S> PeerScoreAdapter<S> {
    pub fn new(score_reader: S) -> Self {
        Self { score_reader }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NopScore<N>(PhantomData<N>);

impl<N> NopScore<N> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<N> monad_peer_score::IdentityScore for NopScore<N> {
    type Identity = N;

    fn score(&self, _identity: &Self::Identity) -> PeerStatus {
        PeerStatus::Unknown
    }
}

impl<N, S> IdentityScore for PeerScoreAdapter<S>
where
    N: Hash + Eq + Send + Sync,
    S: monad_peer_score::IdentityScore<Identity = N>,
{
    type Identity = N;

    fn score(&self, identity: &Self::Identity) -> FragmentPolicy {
        if self.score_reader.score(identity).is_promoted() {
            FragmentPolicy::Prioritized
        } else {
            FragmentPolicy::Regular
        }
    }
}

pub struct LeanRecvMsg<P> {
    pub src_addr: SocketAddr,
    pub payload: Bytes,
    pub public_key: P,
}

pub struct LeanUdpSocketHandle<AP, N, S>
where
    AP: AuthenticationProtocol,
    N: Hash + Eq + Clone + Ord + Send + Sync,
    S: monad_peer_score::IdentityScore<Identity = N>,
{
    auth_socket: AuthenticatedSocketHandle<AP>,
    encoder: Encoder,
    decoder: Decoder<N, PeerScoreAdapter<S>, SystemClock>,
    config: Config,
}

impl<AP, N, S> LeanUdpSocketHandle<AP, N, S>
where
    AP: AuthenticationProtocol,
    N: Hash + Eq + Clone + Ord + Send + Sync,
    S: monad_peer_score::IdentityScore<Identity = N>,
{
    pub fn new(
        auth_socket: AuthenticatedSocketHandle<AP>,
        score_reader: S,
        config: Config,
    ) -> Self {
        let peer_score = PeerScoreAdapter::new(score_reader);
        let (encoder, decoder) = config.clone().build(peer_score);

        Self {
            auth_socket,
            encoder,
            decoder,
            config,
        }
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn connect(
        &mut self,
        remote_public_key: &AP::PublicKey,
        remote_addr: SocketAddr,
        retry_attempts: u64,
    ) -> Result<(), AP::Error> {
        self.auth_socket
            .connect(remote_public_key, remote_addr, retry_attempts)
    }

    pub fn disconnect(&mut self, remote_public_key: &AP::PublicKey) {
        self.auth_socket.disconnect(remote_public_key);
    }

    pub fn flush(&mut self) {
        self.auth_socket.flush();
    }

    pub fn is_connected_socket_and_public_key(
        &self,
        socket_addr: &SocketAddr,
        public_key: &AP::PublicKey,
    ) -> bool {
        self.auth_socket
            .is_connected_socket_and_public_key(socket_addr, public_key)
    }

    pub fn get_socket_by_public_key(&self, public_key: &AP::PublicKey) -> Option<SocketAddr> {
        self.auth_socket.get_socket_by_public_key(public_key)
    }

    pub fn metrics(&self) -> ExecutorMetricsChain<'_> {
        ExecutorMetricsChain::default()
            .push(self.encoder.executor_metrics())
            .push(self.decoder.executor_metrics())
    }
}

impl<AP, N, S> LeanUdpSocketHandle<AP, N, S>
where
    AP: AuthenticationProtocol,
    AP::PublicKey: Clone + Into<N>,
    N: Hash + Eq + Clone + Ord + Send + Sync,
    S: monad_peer_score::IdentityScore<Identity = N>,
{
    pub fn send(&mut self, dst: SocketAddr, payload: Bytes, priority: UdpPriority) {
        let Ok(fragments) = self.encoder.fragment(payload) else {
            return;
        };

        let mut buf =
            BytesMut::with_capacity(PacketHeader::SIZE + self.config.max_fragment_payload);
        for (header, data) in fragments {
            buf.clear();
            buf.put_slice(header.as_bytes());
            buf.put_slice(&data);
            let packet = buf.clone().freeze();
            let stride = packet.len() as u16;
            self.auth_socket.write_unicast_with_priority(
                UnicastMsg {
                    msgs: vec![(dst, packet)],
                    stride,
                },
                priority,
            );
        }
    }

    pub async fn recv(&mut self) -> Result<LeanRecvMsg<AP::PublicKey>, AP::Error> {
        loop {
            let msg = self.auth_socket.recv().await?;

            let Some(public_key) = msg.auth_public_key else {
                continue;
            };

            let identity: N = public_key.into();

            match self.decoder.decode(identity, msg.payload) {
                Ok(DecodeOutcome::Complete(payload)) => {
                    return Ok(LeanRecvMsg {
                        src_addr: msg.src_addr,
                        payload,
                        public_key,
                    });
                }
                Ok(DecodeOutcome::Pending) => continue,
                Err(_) => continue,
            }
        }
    }
}
