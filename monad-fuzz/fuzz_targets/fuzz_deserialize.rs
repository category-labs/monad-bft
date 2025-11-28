// Fuzz runner config:
//
// CORPUS_FILTER=*.deserialize.bin
// TIMEOUT_QUICK=5m
//
// Environments:
//
// AFL_HANG_TMOUT=100
// AFL_EXIT_ON_TIME=300000

use bytes::Bytes;
use monad_bls::BlsSignatureCollection;
use monad_crypto::certificate_signature::CertificateSignaturePubKey;
use monad_eth_types::EthExecutionProtocol;
use monad_raptorcast::message::InboundRouterMessage;
use monad_secp::SecpSignature;
use monad_state::MonadMessage;

type SignatureType = SecpSignature;
type SignatureCollection = BlsSignatureCollection<CertificateSignaturePubKey<SignatureType>>;
type ExecutionProtocol = EthExecutionProtocol;
type Message = MonadMessage<SignatureType, SignatureCollection, ExecutionProtocol>;

#[allow(unused)]
fn main() {
    afl::fuzz!(|data: &[u8]| {
        let app_message = Bytes::copy_from_slice(data);
        let _ = InboundRouterMessage::<Message, SignatureType>::try_deserialize(&app_message);
    });
}
