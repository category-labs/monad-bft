use std::{
    collections::{BTreeMap, HashMap},
    net::{SocketAddr, UdpSocket},
    num::ParseIntError,
    time::Duration,
};

use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use monad_crypto::certificate_signature::{
    CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey,
};
use monad_executor::Executor;
use monad_executor_glue::{Message, RouterCommand};
use monad_raptorcast::{
    udp::build_messages,
    util::{BuildTarget, EpochValidators, Validator},
    RaptorCast, RaptorCastConfig,
};
use monad_secp::SecpSignature;
use monad_types::{Deserializable, Epoch, NodeId, Serializable, Stake};
use tracing_subscriber::fmt::format::FmtSpan;

type SignatureType = SecpSignature;
type PubKeyType = CertificateSignaturePubKey<SignatureType>;

// Try to crash the R10 managed decoder by feeding it encoded symbols of different sizes.
// A previous version of the R10 managed decoder did not handle this correctly and would panic.
#[test]
pub fn different_symbol_sizes() {
    tracing_subscriber::fmt::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_span_events(FmtSpan::CLOSE)
        .init();

    // Cause the test to fail if any of the tokio runtime threads panic.  This code was taken from:
    // https://stackoverflow.com/questions/35988775/how-can-i-cause-a-panic-on-a-thread-to-immediately-end-the-main-thread/36031130#36031130
    let orig_panic_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        orig_panic_hook(panic_info);
        std::process::exit(1);
    }));

    let rx_addr = "127.0.0.1:10000";
    let rx_keypair = {
        <<SignatureType as CertificateSignature>::KeyPairType as CertificateKeyPair>::from_bytes(
            &mut [2; 32],
        )
        .unwrap()
    };
    let rx_nodeid = NodeId::new(rx_keypair.pubkey());

    let tx_addr = "127.0.0.1:10001";
    let tx_keypair = {
        <<SignatureType as CertificateSignature>::KeyPairType as CertificateKeyPair>::from_bytes(
            &mut [1; 32],
        )
        .unwrap()
    };
    let tx_nodeid = NodeId::new(tx_keypair.pubkey());

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let known_addresses: HashMap<NodeId<PubKeyType>, SocketAddr> = HashMap::from([
        (rx_nodeid, rx_addr.parse().unwrap()),
        (tx_nodeid, tx_addr.parse().unwrap()),
    ]);

    let validator_set = vec![(rx_nodeid, Stake(1)), (tx_nodeid, Stake(1))];

    {
        let known_addresses = known_addresses.clone();

        rt.spawn(async move {
            let service_config = RaptorCastConfig {
                key: rx_keypair,
                known_addresses,
                redundancy: 2,
                local_addr: rx_addr.to_owned(),
            };

            let mut service =
                RaptorCast::<SignatureType, MockMessage, MockMessage>::new(service_config);

            service.exec(vec![RouterCommand::AddEpochValidatorSet {
                epoch: Epoch(0),
                validator_set,
            }]);

            loop {
                let message = service.next().await.expect("never terminates");

                println!("received message: {:?}", message);
            }
        });
    }

    // Wait for RaptorCast instance to set itself up.
    std::thread::sleep(Duration::from_secs(1));

    let message: Bytes = vec![0; 100 * 1000].into();

    let tx_socket = UdpSocket::bind(tx_addr).unwrap();

    // Generate differently-sized encoded symbols that look like they are part of the same
    // message.  For the RaptorCast receiver to think that they are part of the same message,
    // they should have the same:
    // - unix_ts_ms: we use 0 for both messages;
    // - author: we use the same tx keypair/nodeid for both messages;
    // - app_message_{hash,len}: we use identical message bodies for the two messages.
    for i in 0..=1 {
        let gso_size = match i {
            0 => 1460,
            1 => 1480,
            _ => panic!(),
        };

        let mut validators = EpochValidators {
            validators: BTreeMap::from([
                (rx_nodeid, Validator { stake: Stake(1) }),
                (tx_nodeid, Validator { stake: Stake(1) }),
            ]),
        };

        let epoch_validators = validators.view_without(vec![&tx_nodeid]);

        let messages = build_messages::<SecpSignature>(
            &tx_keypair,
            gso_size,
            message.clone(),
            2, // redundancy,
            0, // epoch_no
            0, // unix_ts_ms
            BuildTarget::Raptorcast(epoch_validators),
            &known_addresses,
        );

        // Send only the first symbol of the first message, and send all of the symbols
        // in the second message.
        if i == 0 {
            tx_socket
                .send_to(&messages[0].1[0..usize::from(gso_size)], messages[0].0)
                .unwrap();
        } else {
            for message in messages {
                for chunk in message.1.chunks(usize::from(gso_size)) {
                    tx_socket.send_to(chunk, message.0).unwrap();
                }
            }
        }
    }

    // Wait for RaptorCast instance to catch up.
    std::thread::sleep(Duration::from_secs(1));
}

#[derive(Clone, Copy)]
struct MockMessage {
    id: u32,
    message_len: usize,
}

impl MockMessage {
    fn new(id: u32, message_len: usize) -> Self {
        Self { id, message_len }
    }
}

impl Message for MockMessage {
    type NodeIdPubKey = PubKeyType;
    type Event = (NodeId<Self::NodeIdPubKey>, u32);

    fn event(self, from: NodeId<Self::NodeIdPubKey>) -> Self::Event {
        (from, self.id)
    }
}

impl Serializable<Bytes> for MockMessage {
    fn serialize(&self) -> Bytes {
        let mut message = BytesMut::zeroed(self.message_len);
        let id_bytes = self.id.to_le_bytes();
        message[0] = id_bytes[0];
        message[1] = id_bytes[1];
        message[2] = id_bytes[2];
        message[3] = id_bytes[3];
        message.into()
    }
}

impl Deserializable<Bytes> for MockMessage {
    type ReadError = ParseIntError;

    fn deserialize(message: &Bytes) -> Result<Self, Self::ReadError> {
        Ok(Self::new(
            u32::from_le_bytes(message[..4].try_into().unwrap()),
            message.len(),
        ))
    }
}
