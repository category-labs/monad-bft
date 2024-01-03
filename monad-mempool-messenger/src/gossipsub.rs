use std::{error::Error, net::Ipv4Addr};

use libp2p::{
    bytes::Bytes, futures::stream::StreamExt, gossipsub, identity, swarm::SwarmEvent, PeerId, Swarm,
};
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::{event, instrument, Level};

const TOPIC_NAME: &str = "monad-mempool";
pub const GOSSIP_SUB_DEFAULT_BUFFER_SIZE: usize = 64;

#[instrument]
pub fn start_gossipsub<T: 'static + prost::Message + Default>(
    local_key: identity::Keypair,
    bind_address: Ipv4Addr,
    bind_port: u16,
    bootstrap_peers: Vec<(Ipv4Addr, u16)>,
    buffer_size: usize,
) -> Result<
    (
        JoinHandle<()>,
        mpsc::Sender<T>,
        mpsc::Receiver<T>,
        mpsc::Receiver<()>,
    ),
    Box<dyn Error>,
> {
    let topic = gossipsub::IdentTopic::new(TOPIC_NAME);

    let gossipsub_config = gossipsub::ConfigBuilder::default()
        .validation_mode(gossipsub::ValidationMode::Strict)
        .max_transmit_size(usize::MAX)
        .build()
        // The config is fixed, so this should never fail.
        ?;

    let local_peer_id = PeerId::from(local_key.public());

    // Channel for receiving messages from external peers -> local peer
    let (external_tx, external_rx) = mpsc::channel::<T>(buffer_size);
    // Channel for publishing messages from local peer -> external peers
    let (internal_tx, internal_rx) = mpsc::channel::<T>(buffer_size);
    // When the swarm is subscribed to a peer, this channel is notified.
    let (connected_tx, connected_rx) = mpsc::channel::<()>(buffer_size);

    let mut gossipsub = gossipsub::Behaviour::new(
        gossipsub::MessageAuthenticity::Signed(local_key.clone()),
        gossipsub_config,
    )?;
    gossipsub.subscribe(&topic)?;

    let mut swarm = libp2p::SwarmBuilder::with_existing_identity(local_key)
        .with_tokio()
        .with_quic()
        .with_behaviour(|_| gossipsub)?
        .build();

    swarm.listen_on(format!("/ip4/{}/udp/{}/quic-v1", bind_address, bind_port).parse()?)?;
    event! {Level::INFO, "Started listening..."}

    for (dial_address, dial_port) in bootstrap_peers {
        swarm.add_external_address(
            format!("/ip4/{}/udp/{}/quic-v1", dial_address, dial_port).parse()?,
        )
    }

    let listen_handle = tokio::spawn(gossipsub_listen(
        swarm,
        external_tx,
        internal_rx,
        connected_tx,
        topic,
    ));

    Ok((listen_handle, internal_tx, external_rx, connected_rx))
}

async fn gossipsub_listen<T: prost::Message + Default>(
    mut swarm: Swarm<gossipsub::Behaviour>,
    external_tx: mpsc::Sender<T>,
    mut internal_rx: mpsc::Receiver<T>,
    connected_tx: mpsc::Sender<()>,
    topic: gossipsub::IdentTopic,
) {
    loop {
        tokio::select! {
            event = swarm.select_next_some() => {
                match event {
                    SwarmEvent::Behaviour(gossipsub::Event::Message {
                        propagation_source: peer_id,
                        message_id: id,
                        message,
                    }) => {
                        event!(
                            Level::INFO,
                            %peer_id,
                            %id,
                            ?message,
                            "Received message from peer"
                        );
                        match prost::Message::decode(Bytes::from(message.data)) {
                            Ok(decoded) => {
                                if let Err(err) = external_tx.try_send(decoded) {
                                    event!{Level::ERROR, %err, "Failed to send message to channel"};
                                }
                            },
                            Err(err) => {
                                event!{Level::ERROR, %err, "Failed to decode message"}
                            }
                        }
                    },
                    SwarmEvent::Behaviour(gossipsub::Event::Subscribed {
                        ..
                    }) => {
                        if !connected_tx.is_closed() {
                            if let Err(err) = connected_tx.try_send(()) {
                                event!{Level::ERROR, %err, "Failed to notify connected channel"};
                            }
                        }
                    },
                    event => { event!{Level::INFO, ?event} }
                }
            },
            msg = internal_rx.recv() => {
                if let Some(msg) = msg {
                    let encoded = prost::Message::encode_to_vec(&msg);
                    if let Err(err) = swarm.behaviour_mut().publish(topic.clone(), encoded) {
                        event!{Level::ERROR, %err, "Failed to publish message"}
                    }
                }
            },
        }
    }
}
