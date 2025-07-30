use std::{
    marker::PhantomData,
    ops::DerefMut,
    pin::Pin,
    sync::mpsc::Sender,
    task::{Context, Poll},
    time::Duration,
};

use futures::Stream;
use monad_bls::BlsSignatureCollection;
use monad_consensus_types::validator_data::{ValidatorSetData, ValidatorSetDataWithEpoch};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_types::EthExecutionProtocol;
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{MonadEvent, ValSetCommand};
use monad_secp::{PubKey, SecpSignature};
use monad_state_backend::StateBackend;
use monad_types::{SeqNum, Stake};
use monad_validator::signature_collection::{SignatureCollection, SignatureCollectionPubKeyType};
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::info;

/// Updater that gets validator set updates from triedb
pub struct ValSetUpdater<ST, SCT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT> + Send + 'static,
{
    seq_num_send: Sender<SeqNum>,
    next_valset_recv: UnboundedReceiver<(
        Vec<(SCT::NodeIdPubKey, SignatureCollectionPubKeyType<SCT>, Stake)>,
        SeqNum,
    )>,
    last_emitted_val_data: Option<SeqNum>,
    val_set_update_interval: SeqNum,

    metrics: ExecutorMetrics,
    phantom: PhantomData<(ST, SBT)>,
}

impl<SBT> ValSetUpdater<SecpSignature, BlsSignatureCollection<PubKey>, SBT>
where
    SBT: StateBackend<SecpSignature, BlsSignatureCollection<PubKey>> + Send + 'static,
{
    pub fn new(val_set_update_interval: SeqNum, state_backend: SBT) -> Self {
        let (next_valset_send, next_valset_recv) = tokio::sync::mpsc::unbounded_channel();

        let (seq_num_send, seq_num_recv) = std::sync::mpsc::channel();

        std::thread::spawn(move || loop {
            let seq_num_to_read = seq_num_recv.recv().expect("channel never closed");
            while state_backend
                .raw_read_latest_finalized_block()
                .is_none_or(|latest_finalized| latest_finalized < seq_num_to_read)
            {
                info!(?seq_num_to_read, "next valset not ready, going to sleep");
                std::thread::sleep(Duration::from_millis(500));
            }

            let next_valset = state_backend.read_next_valset(seq_num_to_read);

            next_valset_send
                .send((next_valset, seq_num_to_read))
                .expect("channel never closed");
        });
        Self {
            seq_num_send,
            next_valset_recv,
            last_emitted_val_data: None,
            val_set_update_interval,

            metrics: Default::default(),
            phantom: PhantomData,
        }
    }
}

impl<ST, SCT, SBT> Stream for ValSetUpdater<ST, SCT, SBT>
where
    Self: Unpin,
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT> + Send + 'static,
{
    type Item = MonadEvent<ST, SCT, EthExecutionProtocol>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        match this.next_valset_recv.poll_recv(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(maybe_next_valset) => {
                let (validator_set, boundary_block) =
                    maybe_next_valset.expect("channel never closed");

                // validator set data expects (SecpKey, Stake, BlsKey) instead of (SecpKey, BlsKey, Stake)
                let validators = validator_set
                    .into_iter()
                    .map(|(secp_key, bls_key, stake)| (secp_key, stake, bls_key))
                    .collect();
                let validator_set_data: ValidatorSetData<SCT> = ValidatorSetData::new(validators);
                let validator_set_data_with_epoch = ValidatorSetDataWithEpoch {
                    epoch: boundary_block.get_locked_epoch(this.val_set_update_interval),
                    validators: validator_set_data,
                };
                info!(
                    ?validator_set_data_with_epoch,
                    "read validator set from triedb"
                );

                Poll::Ready(Some(MonadEvent::ValidatorEvent(
                    monad_executor_glue::ValidatorEvent::UpdateValidators(
                        validator_set_data_with_epoch,
                    ),
                )))
            }
        }
    }
}

impl<ST, SCT, SBT> Executor for ValSetUpdater<ST, SCT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT> + Send + 'static,
{
    type Command = ValSetCommand;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            match command {
                ValSetCommand::NotifyFinalized(seq_num) => {
                    if seq_num.is_epoch_end(self.val_set_update_interval)
                        && self.last_emitted_val_data != Some(seq_num)
                    {
                        self.seq_num_send
                            .send(seq_num)
                            .expect("channel never closed");
                    }
                }
            }
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        self.metrics.as_ref().into()
    }
}
