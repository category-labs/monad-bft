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

use std::{net::SocketAddr, sync::Arc, time::Duration};

use bytes::Bytes;
use futures::channel::oneshot;
use monad_dataplane::{Dataplane, DataplaneBuilder, TcpMsg, TcpProtocol, TcpSocketId};
use monad_raptorcast::auth::{
    metrics::TCP_METRICS,
    tcp_socket::{wireauth_config, SharedTcpSocket, TcpRecvError},
    WireAuthProtocol,
};
use monad_secp::{KeyPair, PubKey, SecpSignature};

type Socket = SharedTcpSocket<SecpSignature, WireAuthProtocol>;
const DEADLINE: Duration = Duration::from_secs(3);

struct Node {
    dp: Dataplane,
    socket: Socket,
    addr: SocketAddr,
    pk: PubKey,
}

impl Node {
    fn new(seed: u8, config: monad_wireauth::Config) -> Self {
        let key = Arc::new(KeyPair::from_bytes(&mut [seed; 32]).unwrap());
        let mut dp = DataplaneBuilder::new(1000)
            .with_tcp_sockets([(TcpSocketId::Raptorcast, "127.0.0.1:0".parse().unwrap())])
            .build();
        let handle = dp.tcp_sockets.take(TcpSocketId::Raptorcast).unwrap();
        let addr = handle.local_addr();
        let protocol = WireAuthProtocol::new(&TCP_METRICS, config, key.clone());
        Self {
            socket: SharedTcpSocket::new(handle, protocol, key.clone()),
            addr,
            pk: key.pubkey(),
            dp,
        }
    }

    fn send(
        &mut self,
        peer: &Node,
        protocol: TcpProtocol,
        payload: Bytes,
    ) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        self.socket
            .write(peer.addr, &peer.pk, protocol, payload, Some(tx))
            .unwrap();
        rx
    }

    fn metric(&self, name: &str) -> u64 {
        self.socket
            .metrics()
            .into_inner()
            .into_iter()
            .find(|(key, _, _)| *key == name)
            .unwrap()
            .1
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.dp.control.disconnect_ip(self.addr.ip());
    }
}

async fn deliver(sender: &mut Node, receiver: &mut Node, expected: &[u8]) -> SocketAddr {
    tokio::time::timeout(DEADLINE, async {
        let message = tokio::select! {
            result = sender.socket.recv() => panic!("unexpected sender result: {:?}", result.err()),
            result = receiver.socket.recv() => result.unwrap(),
        };
        assert_eq!(message.from, sender.pk);
        assert_eq!(message.payload, expected);
        message.src_addr
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn mixed_protocols_and_reply_share_one_connection() {
    let mut alice = Node::new(1, wireauth_config());
    let mut bob = Node::new(2, wireauth_config());
    for protocol in [
        TcpProtocol::Signature,
        TcpProtocol::WireAuth,
        TcpProtocol::Signature,
        TcpProtocol::WireAuth,
    ] {
        let completion = alice.send(&bob, protocol, Bytes::from_static(b"request"));
        let accepted = deliver(&mut alice, &mut bob, b"request").await;
        completion.await.unwrap();
        let (tx, rx) = oneshot::channel();
        bob.socket
            .write(
                accepted,
                &alice.pk,
                protocol,
                Bytes::from_static(b"reply"),
                Some(tx),
            )
            .unwrap();
        deliver(&mut bob, &mut alice, b"reply").await;
        rx.await.unwrap();
    }
    for (node, name) in [
        (
            &alice,
            "monad.dataplane.tcp.total_outbound_connections_established",
        ),
        (
            &bob,
            "monad.dataplane.tcp.total_inbound_connections_accepted",
        ),
    ] {
        let connections = node
            .dp
            .metrics()
            .iter_with_descriptions()
            .find(|(key, _, _)| *key == name)
            .unwrap()
            .1;
        assert_eq!(connections, 1);
    }
    assert_eq!(
        alice.metric("monad.raptorcast.tcp.signature_bytes_sent"),
        14
    );
    assert_eq!(alice.metric("monad.raptorcast.tcp.wireauth_bytes_sent"), 14);
    assert_eq!(
        bob.metric("monad.wireauth.tcp.state.session_established_responder"),
        1
    );
}

#[tokio::test]
async fn buffer_limit_preserves_accepted_completion_and_cancels_rejected_one() {
    let mut alice = Node::new(3, wireauth_config());
    let mut bob = Node::new(4, wireauth_config());
    let first = Bytes::from(vec![1; 2 * 1024 * 1024]);
    let second = Bytes::from(vec![2; 1024 * 1024 + 200 * 1024]);
    let mut first_completion = alice.send(&bob, TcpProtocol::WireAuth, first.clone());
    let mut second_completion = alice.send(&bob, TcpProtocol::WireAuth, second.clone());
    assert!(first_completion.try_recv().unwrap().is_none());
    assert!(second_completion.try_recv().unwrap().is_none());
    let (tx, rejected) = oneshot::channel();
    assert!(alice
        .socket
        .write(
            bob.addr,
            &bob.pk,
            TcpProtocol::WireAuth,
            Bytes::from_static(b"x"),
            Some(tx)
        )
        .is_err());
    assert!(rejected.await.is_err());
    deliver(&mut alice, &mut bob, &first).await;
    deliver(&mut alice, &mut bob, &second).await;
    first_completion.await.unwrap();
    second_completion.await.unwrap();
    assert_eq!(
        alice.metric("monad.wireauth.tcp.initiator.messages_sent_from_buffer"),
        2
    );
    assert_eq!(alice.metric("monad.raptorcast.tcp.signature_bytes_sent"), 0);
}

#[tokio::test]
async fn closed_connection_does_not_cancel_handshake_until_its_timer_expires() {
    let config = monad_wireauth::Config {
        initiation_timeout: Duration::from_millis(500),
        ..wireauth_config()
    };
    let mut alice = Node::new(5, config.clone());
    let bob = Node::new(6, config);
    let mut completion = alice.send(&bob, TcpProtocol::WireAuth, Bytes::from_static(b"pending"));
    // Bob's dataplane accepts the frame, but its application never dispatches the handshake.
    tokio::time::sleep(Duration::from_millis(30)).await;
    alice.dp.control.disconnect_ip(bob.addr.ip());
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(completion.try_recv().unwrap().is_none());
    assert_eq!(alice.metric("monad.wireauth.tcp.api.disconnect"), 0);
    tokio::time::timeout(DEADLINE, async {
        tokio::select! {
            result = alice.socket.recv() => panic!("unexpected receive: {:?}", result.err()),
            result = &mut completion => assert!(result.is_err()),
        }
    })
    .await
    .unwrap();
    assert_eq!(
        alice.metric("monad.wireauth.tcp.initiator.messages_dropped_from_buffer"),
        1
    );
    assert_eq!(alice.metric("monad.wireauth.tcp.state.total_sessions"), 0);
    assert_eq!(alice.metric("monad.raptorcast.tcp.signature_bytes_sent"), 0);
}

#[tokio::test]
async fn malformed_frames_are_not_exposed_as_application_messages() {
    let mut receiver = Node::new(7, wireauth_config());
    let mut raw = DataplaneBuilder::new(1000)
        .with_tcp_sockets([(TcpSocketId::Raptorcast, "127.0.0.1:0".parse().unwrap())])
        .build();
    let (_, writer) = raw
        .tcp_sockets
        .take(TcpSocketId::Raptorcast)
        .unwrap()
        .split();
    for protocol in [TcpProtocol::Signature, TcpProtocol::WireAuth] {
        writer.write(
            receiver.addr,
            TcpMsg {
                protocol,
                msg: Bytes::from_static(b"invalid"),
                completion: None,
            },
        );
        let (_, error) = tokio::time::timeout(DEADLINE, receiver.socket.recv())
            .await
            .unwrap()
            .err()
            .unwrap();
        match protocol {
            TcpProtocol::Signature => assert!(matches!(error, TcpRecvError::Signature)),
            TcpProtocol::WireAuth => assert!(matches!(error, TcpRecvError::WireAuth(_))),
        }
    }
    assert_eq!(
        receiver.metric("monad.raptorcast.tcp.signature_bytes_received"),
        0
    );
    assert_eq!(
        receiver.metric("monad.raptorcast.tcp.wireauth_bytes_received"),
        0
    );
}
