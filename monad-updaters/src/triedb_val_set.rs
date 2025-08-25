// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::{
    marker::PhantomData,
    ops::DerefMut,
    path::PathBuf,
    pin::Pin,
    sync::mpsc::Sender,
    task::{Context, Poll},
    time::Duration,
};

use futures::Stream;
use monad_bls::BlsSignatureCollection;
use monad_consensus_types::validator_data::{
    ValidatorSetData, ValidatorSetDataWithEpoch, ValidatorsConfig,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_types::EthExecutionProtocol;
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{MonadEvent, ValSetCommand};
use monad_secp::{PubKey, SecpSignature};
use monad_state_backend::StateBackend;
use monad_types::{Epoch, SeqNum};
use monad_validator::signature_collection::SignatureCollection;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::{error, info};

/// Updater that gets validator set updates from triedb
pub struct ValSetUpdater<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    validators_path: PathBuf,

    epoch_length: SeqNum,
    staking_activation: Epoch,

    // sends seq num to state backend thread to initate a valset read
    seq_num_send: Sender<SeqNum>,

    // used by executor until staking activation epoch
    // used by state backend thread after staking activation epoch
    valset_send: UnboundedSender<(ValidatorSetData<SCT>, SeqNum)>,
    valset_recv: UnboundedReceiver<(ValidatorSetData<SCT>, SeqNum)>,

    metrics: ExecutorMetrics,
    phantom: PhantomData<ST>,
}

impl ValSetUpdater<SecpSignature, BlsSignatureCollection<PubKey>> {
    pub fn new<SBT>(
        validators_path: PathBuf,
        epoch_length: SeqNum,
        staking_activation: Epoch,
        state_backend: SBT,
    ) -> Self
    where
        SBT: StateBackend<SecpSignature, BlsSignatureCollection<PubKey>> + Send + 'static,
    {
        let (valset_send, valset_recv) = tokio::sync::mpsc::unbounded_channel();
        let (seq_num_send, seq_num_recv) = std::sync::mpsc::channel();

        let next_valset_send = valset_send.clone();
        std::thread::spawn(move || loop {
            let seq_num_to_read = seq_num_recv.recv().expect("channel never closed");

            // wait until the block is finalized in DB before trying to
            // read the validator set
            while state_backend
                .raw_read_latest_finalized_block()
                .is_none_or(|latest_finalized| latest_finalized < seq_num_to_read)
            {
                info!(?seq_num_to_read, "next valset not ready, sleeping");
                std::thread::sleep(Duration::from_millis(500));
            }

            let next_valset = state_backend.read_next_valset(seq_num_to_read);

            // validator set data expects (SecpKey, Stake, BlsKey) instead of (SecpKey, BlsKey, Stake)
            let validators = next_valset
                .into_iter()
                .map(|(secp_key, bls_key, stake)| (secp_key, stake, bls_key))
                .collect();
            let validator_set_data = ValidatorSetData::new(validators);

            info!(
                ?seq_num_to_read,
                ?validator_set_data,
                "read next validator set from triedb"
            );

            next_valset_send
                .send((validator_set_data, seq_num_to_read))
                .expect("channel never closed");
        });

        Self {
            validators_path,

            epoch_length,
            staking_activation,

            seq_num_send,

            valset_send,
            valset_recv,

            metrics: Default::default(),
            phantom: PhantomData,
        }
    }
}

impl<ST, SCT> ValSetUpdater<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn valset_update_from_validators_toml(&mut self, seq_num: SeqNum) {
        let locked_epoch = seq_num.get_locked_epoch(self.epoch_length);
        assert_eq!(locked_epoch, seq_num.to_epoch(self.epoch_length) + Epoch(1));
        let validator_set_data = ValidatorsConfig::read_from_path(&self.validators_path)
            // I'm hesitant to provide any fallback for this, because
            // having the wrong validator set can be catastrophic.
            //
            // This file should never be manually edited anyways.
            .expect("failed to read validators_path")
            .get_validator_set(&locked_epoch)
            .clone();
        self.valset_send
            .send((validator_set_data, seq_num))
            .expect("channel never closed");
    }
}

impl<ST, SCT> Stream for ValSetUpdater<ST, SCT>
where
    Self: Unpin,
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    type Item = MonadEvent<ST, SCT, EthExecutionProtocol>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        match this.valset_recv.poll_recv(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(maybe_next_valset) => {
                let (validator_set_data, boundary_block) =
                    maybe_next_valset.expect("channel never closed");

                assert!(boundary_block.is_epoch_end(this.epoch_length));
                let validator_set_data_with_epoch = ValidatorSetDataWithEpoch {
                    epoch: boundary_block.get_locked_epoch(this.epoch_length),
                    validators: validator_set_data,
                };
                info!(
                    ?validator_set_data_with_epoch,
                    "received validator set data"
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

impl<ST, SCT> Executor for ValSetUpdater<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    type Command = ValSetCommand;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            match command {
                ValSetCommand::NotifyFinalized(seq_num) => {
                    if seq_num.is_epoch_end(self.epoch_length) {
                        if !self.valset_recv.is_empty() {
                            error!("Validator set data is not consumed");
                        }

                        if seq_num.get_locked_epoch(self.epoch_length) >= self.staking_activation {
                            self.seq_num_send
                                .send(seq_num)
                                .expect("channel never closed");
                        } else {
                            self.valset_update_from_validators_toml(seq_num);
                        }
                    }
                }
            }
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        self.metrics.as_ref().into()
    }
}
