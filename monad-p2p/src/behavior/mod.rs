use libp2p::{request_response::ProtocolSupport, swarm::NetworkBehaviour};
use monad_executor::{Message, Serializable};

mod codec;

#[derive(NetworkBehaviour)]
pub(crate) struct Behavior<M>
where
    M: Message + Serializable,
{
    pub identify: libp2p::identify::Behaviour,
    pub request_response: libp2p::request_response::Behaviour<codec::ReliableMessageCodec<M>>,
}
const IDENTIFY_PROTO_NAME: &str = "/monad/identify/0.0.1";
const REQUEST_RESPONSE_PROTO_NAME: &str = "/monad/req-res/0.0.1";
const REQUEST_RESPONSE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);
const REQUEST_RESPONSE_KEEPALIVE: std::time::Duration = std::time::Duration::from_secs(1);

impl<M> Behavior<M>
where
    M: Message + Serializable,
{
    pub(crate) fn new(identity: &libp2p::identity::Keypair) -> Self {
        let identify = libp2p::identify::Behaviour::new(libp2p::identify::Config::new(
            IDENTIFY_PROTO_NAME.to_string(),
            identity.public(),
        ));

        let mut request_response_config = libp2p::request_response::Config::default();
        request_response_config
            .set_request_timeout(REQUEST_RESPONSE_TIMEOUT)
            .set_connection_keep_alive(REQUEST_RESPONSE_KEEPALIVE);
        let request_response = libp2p::request_response::Behaviour::new(
            codec::ReliableMessageCodec::default(),
            [(
                REQUEST_RESPONSE_PROTO_NAME.to_owned(),
                ProtocolSupport::Full,
            )],
            request_response_config,
        );

        Self {
            identify,
            request_response,
        }
    }
}
