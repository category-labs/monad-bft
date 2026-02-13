use bytes::Bytes;
use monad_executor_glue::{Message, OutboundForwardTxs};
use monad_types::NodeId;

#[derive(Clone, Debug, alloy_rlp::RlpEncodable, alloy_rlp::RlpDecodable)]
pub struct TxIntegrationMessage(#[rlp(trailing)] pub Vec<Bytes>);

#[derive(Clone, Debug)]
pub struct TxIntegrationEvent {
    pub from: NodeId<monad_secp::PubKey>,
    pub txs: Vec<Bytes>,
}

impl Message for TxIntegrationMessage {
    type NodeIdPubKey = monad_secp::PubKey;
    type Event = TxIntegrationEvent;

    fn event(self, from: NodeId<Self::NodeIdPubKey>) -> Self::Event {
        TxIntegrationEvent { from, txs: self.0 }
    }
}

impl OutboundForwardTxs for TxIntegrationMessage {
    fn forward_txs(txs: Vec<Bytes>) -> Self {
        Self(txs)
    }
}
