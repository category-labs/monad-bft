use std::{
    marker::PhantomData,
    pin::Pin,
    sync::{
        mpsc::{Receiver, Sender},
        Arc, Mutex,
    },
    task::Poll,
};

use alloy_rlp::{Decodable, Encodable};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_dataplane::Dataplane;
use monad_executor::{Executor, ExecutorMetricsChain};
use monad_executor_glue::{Message, RouterCommand};
use monad_raptorcast::{
    config::{RaptorCastConfig, SecondaryRcModeConfig},
    raptorcast_secondary::{group_message::FullNodesGroupMessage, RaptorCastSecondary},
    util::Group,
    RaptorCast, RaptorCastEvent,
};
use monad_types::{Deserializable, Serializable};

//==============================================================================
pub struct MultiRouter<ST, M, OM, SE>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Deserializable<Bytes> + Decodable,
    OM: Serializable<Bytes> + Encodable + Into<M> + Clone,
{
    _shared_dataplane: Arc<Mutex<Dataplane>>,
    _rc_validators: RaptorCast<ST, M, OM, SE>,
    _rc_full_nodes: Option<RaptorCastSecondary<ST, M, OM, SE>>,
    _phantom: PhantomData<(OM, SE)>,
}

impl<ST, M, OM, SE> MultiRouter<ST, M, OM, SE>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Deserializable<Bytes> + Decodable,
    OM: Serializable<Bytes> + Encodable + Into<M> + Clone,
{
    pub fn new(cfg: &RaptorCastConfig<ST>, shared_dataplane: Arc<Mutex<Dataplane>>) -> Self {
        // Crete a channel where the primary RC (RaptorCast) instance can
        // forward inbound FullNodesGroupMessage to the secondary one.
        type FullNodesGroupMsg<ST> = FullNodesGroupMessage<ST>;
        type RcToRcChannelMsg<ST> = (
            Sender<FullNodesGroupMsg<ST>>,
            Receiver<FullNodesGroupMsg<ST>>,
        );
        type RcToRcChannelGrp<ST> = (Sender<Group<ST>>, Receiver<Group<ST>>);
        let (txm, rxm): RcToRcChannelMsg<ST> = std::sync::mpsc::channel();
        let (txg, rxg): RcToRcChannelGrp<ST> = std::sync::mpsc::channel();
        let _rc_full_nodes = match &cfg.secondary_instance {
            SecondaryRcModeConfig::None => None,
            _ => Some(RaptorCastSecondary::new(
                cfg,
                shared_dataplane.clone(),
                rxm,
                txg,
            )),
        };

        Self {
            _shared_dataplane: shared_dataplane.clone(),
            _rc_validators: RaptorCast::new(cfg, shared_dataplane)
                .bind_channel_to_secondary_raptorcast(txm, rxg),
            _rc_full_nodes,
            _phantom: PhantomData,
        }
    }
}

//==============================================================================
impl<ST, M, OM, SE> Executor for MultiRouter<ST, M, OM, SE>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Deserializable<Bytes> + Decodable,
    OM: Serializable<Bytes> + Encodable + Into<M> + Clone,
    RaptorCast<ST, M, OM, SE>: Unpin,
{
    type Command = RouterCommand<CertificateSignaturePubKey<ST>, OM>;

    fn exec(&mut self, a_commands: Vec<Self::Command>) {
        let mut validator_cmds = Vec::new();
        let mut fullnodes_cmds = Vec::new();
        for cmd in a_commands {
            match cmd {
                RouterCommand::Publish { .. } => validator_cmds.push(cmd),
                RouterCommand::AddEpochValidatorSet { .. } => validator_cmds.push(cmd),
                RouterCommand::GetPeers => validator_cmds.push(cmd),
                RouterCommand::UpdatePeers { .. } => validator_cmds.push(cmd),

                RouterCommand::PublishToFullNodes { .. } => fullnodes_cmds.push(cmd),
                RouterCommand::GetFullNodes => fullnodes_cmds.push(cmd),
                RouterCommand::UpdateFullNodes { .. } => fullnodes_cmds.push(cmd),

                RouterCommand::UpdateCurrentRound(epoch, round) => {
                    let cmd_cpy = RouterCommand::UpdateCurrentRound(epoch, round);
                    validator_cmds.push(cmd);
                    fullnodes_cmds.push(cmd_cpy);
                }
            }
        }
        self._rc_validators.exec(validator_cmds);
        if self._rc_full_nodes.is_some() {
            self._rc_full_nodes.as_mut().unwrap().exec(fullnodes_cmds);
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        let m1 = self._rc_validators.metrics();
        let res: ExecutorMetricsChain = if self._rc_full_nodes.is_some() {
            let m2 = self._rc_full_nodes.as_ref().unwrap().metrics();
            m1.chain(m2)
        } else {
            m1
        };
        res
    }
}

//==============================================================================
impl<ST, M, OM, E> Stream for MultiRouter<ST, M, OM, E>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Deserializable<Bytes> + Decodable,
    OM: Serializable<Bytes> + Encodable + Into<M> + Clone,
    E: From<RaptorCastEvent<M::Event, CertificateSignaturePubKey<ST>>>,
    Self: Unpin,
    RaptorCast<ST, M, OM, E>: Unpin,
    RaptorCastSecondary<ST, M, OM, E>: Unpin,
{
    type Item = E;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let pinned_this = self.as_mut().get_mut();

        // Primary RC instance polls for inbound TCP, UDP raptorcast
        // and FullNodesGroupMessage intended for the secondary RC instance.
        match pinned_this._rc_validators.poll_next_unpin(cx) {
            Poll::Ready(Some(event)) => return Poll::Ready(Some(event)),
            Poll::Ready(None) => {}
            Poll::Pending => {}
        }

        // Secondary RC instance polls for FullNodesGroupMessage coming in from
        // the Channel Primary->Secondary.
        if self._rc_full_nodes.is_some() {
            let fn_stream = self._rc_full_nodes.as_mut().unwrap();
            match fn_stream.poll_next_unpin(cx) {
                Poll::Ready(Some(event)) => return Poll::Ready(Some(event)),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => {}
            }
        }

        Poll::Pending
    }
}
