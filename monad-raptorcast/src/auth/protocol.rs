use std::{convert::TryFrom, net::SocketAddr, sync::Arc, time::Instant};

use bytes::Bytes;
use monad_crypto::certificate_signature::PubKey;
use monad_executor::ExecutorMetricsChain;
use monad_wireauth::{
    messages::{DataPacketHeader, Packet},
    ProtocolErrorContext,
};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

pub trait AuthenticationProtocol {
    type PublicKey: PubKey;
    type Error: std::fmt::Debug;
    type Header: IntoBytes + Immutable;

    const HEADER_SIZE: u16;

    fn connect(
        &mut self,
        remote_public_key: &Self::PublicKey,
        remote_addr: SocketAddr,
        retry_attempts: u64,
    ) -> Result<(), Self::Error>;

    fn disconnect(&mut self, remote_public_key: &Self::PublicKey);

    fn dispatch(
        &mut self,
        packet: &mut [u8],
        remote_addr: SocketAddr,
    ) -> Result<Option<(Bytes, Self::PublicKey)>, Self::Error>;

    fn encrypt_by_public_key(
        &mut self,
        public_key: &Self::PublicKey,
        plaintext: &mut [u8],
    ) -> Result<Self::Header, Self::Error>;

    fn encrypt_by_socket(
        &mut self,
        socket_addr: &SocketAddr,
        plaintext: &mut [u8],
    ) -> Result<Self::Header, Self::Error>;

    fn is_connected_public_key(&self, public_key: &Self::PublicKey) -> bool;

    fn is_connected_socket(&self, socket_addr: &SocketAddr) -> bool;

    fn is_connected_socket_and_public_key(
        &self,
        socket_addr: &SocketAddr,
        public_key: &Self::PublicKey,
    ) -> bool;

    fn get_socket_by_public_key(&self, public_key: &Self::PublicKey) -> Option<SocketAddr>;

    fn has_any_session_by_public_key(&self, public_key: &Self::PublicKey) -> bool;

    fn next_packet(&mut self) -> Option<(SocketAddr, Bytes)>;

    fn tick(&mut self);

    fn next_deadline(&self) -> Option<Instant>;

    fn metrics(&self) -> ExecutorMetricsChain;
}

pub struct WireAuthProtocol {
    api: monad_wireauth::API<monad_wireauth::StdContext>,
}

impl WireAuthProtocol {
    pub fn new(config: monad_wireauth::Config, signing_key: &Arc<monad_secp::KeyPair>) -> Self {
        let context = monad_wireauth::StdContext::new();
        let mut secret_bytes = signing_key.secret_bytes();
        let keypair = monad_secp::KeyPair::from_bytes(&mut secret_bytes).expect("valid keypair");

        Self {
            api: monad_wireauth::API::new(config, keypair, context),
        }
    }
}

impl AuthenticationProtocol for WireAuthProtocol {
    type PublicKey = monad_secp::PubKey;
    type Error = monad_wireauth::Error;
    type Header = monad_wireauth::messages::DataPacketHeader;

    const HEADER_SIZE: u16 = DataPacketHeader::SIZE as u16;

    fn connect(
        &mut self,
        remote_public_key: &Self::PublicKey,
        remote_addr: SocketAddr,
        retry_attempts: u64,
    ) -> Result<(), Self::Error> {
        self.api
            .connect(*remote_public_key, remote_addr, retry_attempts)
    }

    fn disconnect(&mut self, remote_public_key: &Self::PublicKey) {
        self.api.disconnect(remote_public_key)
    }

    fn dispatch(
        &mut self,
        packet: &mut [u8],
        remote_addr: SocketAddr,
    ) -> Result<Option<(Bytes, Self::PublicKey)>, Self::Error> {
        match Packet::try_from(packet).map_err(|e| e.with_addr(remote_addr))? {
            Packet::Control(control_packet) => {
                self.api.dispatch_control(control_packet, remote_addr)?;
                Ok(None)
            }
            Packet::Data(data_packet) => {
                let (plaintext, public_key) = self.api.decrypt(data_packet, remote_addr)?;
                Ok(Some((
                    Bytes::copy_from_slice(plaintext.as_ref()),
                    public_key,
                )))
            }
        }
    }

    fn encrypt_by_public_key(
        &mut self,
        public_key: &Self::PublicKey,
        plaintext: &mut [u8],
    ) -> Result<Self::Header, Self::Error> {
        self.api.encrypt_by_public_key(public_key, plaintext)
    }

    fn encrypt_by_socket(
        &mut self,
        socket_addr: &SocketAddr,
        plaintext: &mut [u8],
    ) -> Result<Self::Header, Self::Error> {
        self.api.encrypt_by_socket(socket_addr, plaintext)
    }

    fn next_packet(&mut self) -> Option<(SocketAddr, Bytes)> {
        self.api.next_packet()
    }

    fn tick(&mut self) {
        self.api.tick();
    }

    fn next_deadline(&self) -> Option<Instant> {
        self.api.next_deadline()
    }

    fn is_connected_public_key(&self, public_key: &Self::PublicKey) -> bool {
        self.api.is_connected_public_key(public_key)
    }

    fn is_connected_socket(&self, socket_addr: &SocketAddr) -> bool {
        self.api.is_connected_socket(socket_addr)
    }

    fn is_connected_socket_and_public_key(
        &self,
        socket_addr: &SocketAddr,
        public_key: &Self::PublicKey,
    ) -> bool {
        self.api
            .is_connected_socket_and_public_key(socket_addr, public_key)
    }

    fn get_socket_by_public_key(&self, public_key: &Self::PublicKey) -> Option<SocketAddr> {
        self.api.get_socket_by_public_key(public_key)
    }

    fn has_any_session_by_public_key(&self, public_key: &Self::PublicKey) -> bool {
        self.api.has_any_session_by_public_key(public_key)
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        self.api.metrics()
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, FromBytes, IntoBytes, Immutable, KnownLayout)]
pub struct NoopHeader;

pub struct NoopAuthProtocol<P: PubKey> {
    _phantom: std::marker::PhantomData<P>,
}

impl<P: PubKey> NoopAuthProtocol<P> {
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<P: PubKey> Default for NoopAuthProtocol<P> {
    fn default() -> Self {
        Self::new()
    }
}

impl<P: PubKey> AuthenticationProtocol for NoopAuthProtocol<P> {
    type PublicKey = P;
    type Error = std::convert::Infallible;
    type Header = NoopHeader;

    const HEADER_SIZE: u16 = 0;

    fn connect(
        &mut self,
        _remote_public_key: &Self::PublicKey,
        _remote_addr: SocketAddr,
        _retry_attempts: u64,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn disconnect(&mut self, _remote_public_key: &Self::PublicKey) {}

    fn dispatch(
        &mut self,
        packet: &mut [u8],
        _remote_addr: SocketAddr,
    ) -> Result<Option<(Bytes, Self::PublicKey)>, Self::Error> {
        let dummy_pubkey: P = unsafe { std::mem::zeroed() };
        Ok(Some((Bytes::copy_from_slice(packet), dummy_pubkey)))
    }

    fn encrypt_by_public_key(
        &mut self,
        _public_key: &Self::PublicKey,
        _plaintext: &mut [u8],
    ) -> Result<Self::Header, Self::Error> {
        Ok(NoopHeader)
    }

    fn encrypt_by_socket(
        &mut self,
        _socket_addr: &SocketAddr,
        _plaintext: &mut [u8],
    ) -> Result<Self::Header, Self::Error> {
        Ok(NoopHeader)
    }

    fn next_packet(&mut self) -> Option<(SocketAddr, Bytes)> {
        None
    }

    fn tick(&mut self) {}

    fn next_deadline(&self) -> Option<Instant> {
        None
    }

    fn is_connected_public_key(&self, _public_key: &Self::PublicKey) -> bool {
        false
    }

    fn is_connected_socket(&self, _socket_addr: &SocketAddr) -> bool {
        false
    }

    fn is_connected_socket_and_public_key(
        &self,
        _socket_addr: &SocketAddr,
        _public_key: &Self::PublicKey,
    ) -> bool {
        false
    }

    fn get_socket_by_public_key(&self, _public_key: &Self::PublicKey) -> Option<SocketAddr> {
        None
    }

    fn has_any_session_by_public_key(&self, _public_key: &Self::PublicKey) -> bool {
        false
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        ExecutorMetricsChain::default()
    }
}
