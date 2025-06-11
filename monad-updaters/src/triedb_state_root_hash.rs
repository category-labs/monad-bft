use std::{
    marker::PhantomData,
    ops::DerefMut,
    path::{Path, PathBuf},
    pin::Pin,
    task::{Context, Poll, Waker},
};

use futures::Stream;
use monad_consensus_types::{
    signature_collection::SignatureCollection,
    validator_data::{ValidatorSetDataWithEpoch, ValidatorsConfig},
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_types::EthExecutionProtocol;
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{MonadEvent, StateRootHashCommand};
use monad_types::{Epoch, SeqNum};
use tracing::error;

/// Updater that gets state root hash updates by polling triedb
pub struct StateRootHashTriedbPoll<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    validators_path: PathBuf,

    next_val_data: Option<ValidatorSetDataWithEpoch<SCT>>,
    last_emitted_val_data: Option<SeqNum>,
    val_set_update_interval: SeqNum,

    waker: Option<Waker>,
    metrics: ExecutorMetrics,
    phantom: PhantomData<(ST, SCT)>,
}

impl<ST, SCT> StateRootHashTriedbPoll<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    pub fn new(
        triedb_path: &Path,
        validators_path: &Path,
        val_set_update_interval: SeqNum,
    ) -> Self {
        // assert that validators_path is accessible
        let _: ValidatorsConfig<SCT> = ValidatorsConfig::read_from_path(validators_path)
            .expect("failed to read validators_path");

        // TODO read validator set from triedb
        let _path = triedb_path.to_path_buf();

        Self {
            validators_path: validators_path.to_owned(),

            next_val_data: None,
            last_emitted_val_data: None,
            val_set_update_interval,

            waker: None,
            metrics: Default::default(),
            phantom: PhantomData,
        }
    }

    fn valset_update(&mut self, seq_num: SeqNum) {
        if seq_num.is_epoch_end(self.val_set_update_interval)
            && self.last_emitted_val_data != Some(seq_num)
        {
            if self.next_val_data.is_some() {
                error!("Validator set data is not consumed");
            }
            let locked_epoch = seq_num.get_locked_epoch(self.val_set_update_interval);
            assert_eq!(
                locked_epoch,
                seq_num.to_epoch(self.val_set_update_interval) + Epoch(2)
            );
            self.next_val_data = Some(ValidatorSetDataWithEpoch {
                epoch: locked_epoch,
                validators: ValidatorsConfig::read_from_path(&self.validators_path)
                    // I'm hesitant to provide any fallback for this, because
                    // having the wrong validator set can be catastrophic.
                    //
                    // This file should never be manually edited anyways.
                    .expect("failed to read validators_path")
                    .get_validator_set(&locked_epoch)
                    .clone(),
            });
            self.last_emitted_val_data = Some(seq_num);
        }
    }
}

impl<ST, SCT> Stream for StateRootHashTriedbPoll<ST, SCT>
where
    Self: Unpin,
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    type Item = MonadEvent<ST, SCT, EthExecutionProtocol>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        if this.waker.is_none() {
            this.waker = Some(cx.waker().clone());
        }

        if let Some(next_val_data) = this.next_val_data.take() {
            return Poll::Ready(Some(MonadEvent::ValidatorEvent(
                monad_executor_glue::ValidatorEvent::UpdateValidators(next_val_data),
            )));
        }

        Poll::Pending
    }
}

impl<ST, SCT> Executor for StateRootHashTriedbPoll<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    type Command = StateRootHashCommand;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        let mut wake = false;

        for command in commands {
            match command {
                StateRootHashCommand::NotifyFinalized(seq_num) => {
                    self.valset_update(seq_num);
                    wake = true;
                }
            }
        }

        if wake {
            if let Some(waker) = self.waker.take() {
                waker.wake()
            }
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        self.metrics.as_ref().into()
    }
}
