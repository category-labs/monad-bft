use std::error::Error;

use libp2p::{
    bytes::Bytes,
    futures::stream::StreamExt,
    gossipsub, identity, mdns,
    swarm::{dial_opts::DialOpts, NetworkBehaviour, SwarmBuilder, SwarmEvent},
    PeerId, Swarm,
};
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::{event, instrument, Level};

const TOPIC_NAME: &str = "monad-mempool";
pub const GOSSIP_SUB_DEFAULT_BUFFER_SIZE: usize = 64;

#[derive(NetworkBehaviour)]
struct GossipSubBehavior {
    gossipsub: gossipsub::Behaviour,
    mdns: mdns::tokio::Behaviour,
}

#[instrument]
pub fn start_gossipsub<T: 'static + prost::Message + Default>(
    local_key: identity::Keypair,
    port: u16,
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

    let mdns_config = mdns::Config::default();

    let local_peer_id = PeerId::from(local_key.public());

    // Channel for receiving messages from external peers -> local peer
    let (external_tx, external_rx) = mpsc::channel::<T>(buffer_size);
    // Channel for publishing messages from local peer -> external peers
    let (internal_tx, internal_rx) = mpsc::channel::<T>(buffer_size);
    // When the swarm is subscribed to a peer, this channel is notified.
    let (connected_tx, connected_rx) = mpsc::channel::<()>(buffer_size);

    // TODO: Use a custom transport.
    let transport = libp2p::tokio_development_transport(local_key.clone())?;

    let mdns = mdns::tokio::Behaviour::new(mdns_config, local_peer_id)?;

    let mut gossipsub = gossipsub::Behaviour::new(
        gossipsub::MessageAuthenticity::Signed(local_key),
        gossipsub_config,
    )?;
    gossipsub.subscribe(&topic)?;

    let behavior = GossipSubBehavior { gossipsub, mdns };

    let mut swarm = SwarmBuilder::with_tokio_executor(transport, behavior, local_peer_id).build();

    swarm.listen_on(format!("/ip4/0.0.0.0/tcp/{port}", port = port).parse()?)?;
    event! {Level::INFO, "Started listening..."}

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
    mut swarm: Swarm<GossipSubBehavior>,
    external_tx: mpsc::Sender<T>,
    mut internal_rx: mpsc::Receiver<T>,
    connected_tx: mpsc::Sender<()>,
    topic: gossipsub::IdentTopic,
) {
    loop {
        tokio::select! {
            event = swarm.select_next_some() => {
                match event {
                    SwarmEvent::Behaviour(GossipSubBehaviorEvent::Mdns(mdns::Event::Discovered(list))) => {
                        for (peer_id, _multiaddr) in list {
                            event!(Level::INFO, %peer_id, "mDNS discovered new peer");
                            if let Err(e) = swarm.dial(DialOpts::peer_id(peer_id).build()) {
                                event!{Level::ERROR, %e, "Failed to dial peer"};
                            }
                        }
                    },
                    SwarmEvent::Behaviour(GossipSubBehaviorEvent::Mdns(mdns::Event::Expired(list))) => {
                        for (peer_id, _multiaddr) in list {
                            event!(Level::INFO, %peer_id, "mDNS discover peer has expired");
                            // We do not need to explicitly remove peers because this is handled by gossipsub.
                        }
                    },
                    SwarmEvent::Behaviour(GossipSubBehaviorEvent::Gossipsub(gossipsub::Event::Message {
                        propagation_source: peer_id,
                        message_id: id,
                        message,
                    })) => {
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
                    SwarmEvent::Behaviour(GossipSubBehaviorEvent::Gossipsub(gossipsub::Event::Subscribed {
                        ..
                    })) => {
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
                    if let Err(err) = swarm.behaviour_mut().gossipsub.publish(topic.clone(), encoded) {
                        event!{Level::ERROR, %err, "Failed to publish message"}
                    }
                }
            },
        }
    }
}
