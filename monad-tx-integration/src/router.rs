use alloy_consensus::TxEnvelope;
use alloy_rlp::Encodable;
use bytes::Bytes;
use monad_chain_config::{revision::MockChainRevision, MockChainConfig};
use monad_eth_block_policy::EthBlockPolicy;
use monad_executor_glue::TxPoolCommand;
use monad_secp::SecpSignature;
use monad_state_backend::InMemoryState;
use monad_testutil::signing::MockSignatures;
use monad_types::NodeId;

pub type ST = SecpSignature;
pub type SCT = MockSignatures<SecpSignature>;
pub type SBT = InMemoryState<ST, SCT>;
pub type CCT = MockChainConfig;
pub type CRT = MockChainRevision;

pub type Command = TxPoolCommand<
    ST,
    SCT,
    monad_eth_types::EthExecutionProtocol,
    EthBlockPolicy<ST, SCT, CCT, CRT>,
    SBT,
    CCT,
    CRT,
>;

pub fn pubkey_to_node_id(pubkey: &monad_secp::PubKey) -> NodeId<monad_secp::PubKey> {
    NodeId::new(*pubkey)
}

pub fn route_lean_udp_tx(sender_pubkey: &monad_secp::PubKey, tx: &TxEnvelope) -> Command {
    let node_id = pubkey_to_node_id(sender_pubkey);
    let mut buf = bytes::BytesMut::new();
    tx.encode(&mut buf);
    TxPoolCommand::InsertForwardedTxs {
        sender: node_id,
        txs: vec![Bytes::from(buf.freeze())],
    }
}

pub fn route_forward_txs(sender_pubkey: &monad_secp::PubKey, txs: Vec<Bytes>) -> Command {
    let node_id = pubkey_to_node_id(sender_pubkey);
    TxPoolCommand::InsertForwardedTxs {
        sender: node_id,
        txs,
    }
}
