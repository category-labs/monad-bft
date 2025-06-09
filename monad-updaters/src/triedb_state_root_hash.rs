use std::{
    collections::HashMap,
    marker::PhantomData,
    ops::DerefMut,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

use futures::Stream;
use monad_consensus_types::{
    block::{ExecutionResult, ProposedExecutionResult},
    signature_collection::SignatureCollection,
    validator_data::{ValidatorSetDataWithEpoch, ValidatorsConfig},
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_types::EthExecutionProtocol;
use monad_executor::Executor;
use monad_executor_glue::{MonadEvent, StateRootHashCommand};
use monad_triedb_utils::TriedbReader;
use monad_types::{BlockId, Epoch, Round, SeqNum};
use tracing::{debug, error, trace, warn};

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum StateRootRequest {
    Proposed {
        block_id: BlockId,
        seq_num: SeqNum,
        round: Round,
    },
    Finalized {
        seq_num: SeqNum,
    },
}

impl StateRootRequest {
    fn seq_num(&self) -> &SeqNum {
        match self {
            Self::Proposed { seq_num, .. } => seq_num,
            Self::Finalized { seq_num, .. } => seq_num,
        }
    }
}

/// Updater that gets state root hash updates by polling triedb
pub struct StateRootHashTriedbPoll<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    validators_path: PathBuf,

    triedb_recv: tokio::sync::mpsc::UnboundedReceiver<ExecutionResult<EthExecutionProtocol>>,
    state_root_requests_send: std::sync::mpsc::Sender<StateRootRequest>,
    cancel_below: Arc<Mutex<SeqNum>>,

    next_val_data: Option<ValidatorSetDataWithEpoch<SCT>>,
    last_emitted_val_data: Option<SeqNum>,
    val_set_update_interval: SeqNum,

    waker: Option<Waker>,
    phantom: PhantomData<ST>,
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

        let (triedb_send, triedb_recv) = tokio::sync::mpsc::unbounded_channel();

        let (state_root_requests_send, state_root_requests_recv) =
            std::sync::mpsc::channel::<StateRootRequest>();
        let cancel_below = Arc::new(Mutex::new(SeqNum(0)));
        let cancel_below_clone = cancel_below.clone();

        let path = triedb_path.to_path_buf();
        rayon::spawn(move || {
            // FIXME: handle error, maybe retry
            let handle = TriedbReader::try_new(path.as_path()).unwrap();

            let mut outstanding_requests: HashMap<_, usize> = HashMap::new();

            loop {
                while let Ok(request) = state_root_requests_recv.try_recv() {
                    outstanding_requests.insert(request, 0);
                }
                let cancel_below = *cancel_below_clone.lock().unwrap();
                outstanding_requests.retain(|request, _| request.seq_num() >= &cancel_below);

                let mut successes = Vec::new();
                for (request, num_tries) in &mut outstanding_requests {
                    let maybe_event = match request {
                        StateRootRequest::Proposed {
                            block_id,
                            seq_num,
                            round,
                        } => handle
                            .get_proposed_eth_header(block_id, seq_num, round)
                            .map(|header| {
                                ExecutionResult::Proposed(ProposedExecutionResult {
                                    block_id: *block_id,
                                    seq_num: *seq_num,
                                    round: *round,
                                    result: header,
                                })
                            }),
                        StateRootRequest::Finalized { seq_num } => handle
                            .get_finalized_eth_header(seq_num)
                            .map(|header| ExecutionResult::Finalized(*seq_num, header)),
                    };
                    trace!(result =? maybe_event, "polled eth_header");
                    if let Some(event) = maybe_event {
                        triedb_send.send(event).unwrap();
                        successes.push(*request);
                        continue;
                    }

                    *num_tries += 1;

                    if *num_tries > 1 {
                        warn!(?request, ?num_tries, "no eth header");
                    } else {
                        debug!(?request, ?num_tries, "no eth header");
                    }
                }

                for success in successes {
                    outstanding_requests
                        .remove(&success)
                        .expect("failed to remove success");
                }

                std::thread::sleep(std::time::Duration::from_millis(50));
            }
        });

        Self {
            validators_path: validators_path.to_owned(),

            triedb_recv,
            cancel_below,
            state_root_requests_send,
            next_val_data: None,
            last_emitted_val_data: None,
            val_set_update_interval,

            waker: None,
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

        let s = this.triedb_recv.poll_recv(cx);
        s.map(|s| s.map(|event| MonadEvent::ExecutionResultEvent(event)))
    }
}

impl<ST, SCT> Executor for StateRootHashTriedbPoll<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    type Command = StateRootHashCommand;
    type Metrics = ();

    fn exec(&mut self, commands: Vec<Self::Command>) {
        let mut wake = false;

        for command in commands {
            match command {
                StateRootHashCommand::CancelBelow(seq_num) => {
                    *self.cancel_below.lock().unwrap() = seq_num;
                }
                StateRootHashCommand::RequestProposed(block_id, seq_num, round) => {
                    self.state_root_requests_send
                        .send(StateRootRequest::Proposed {
                            block_id,
                            seq_num,
                            round,
                        })
                        .expect("state_root_requests receiver should never be dropped");
                    wake = true;
                }
                StateRootHashCommand::RequestFinalized(seq_num) => {
                    self.valset_update(seq_num);
                    self.state_root_requests_send
                        .send(StateRootRequest::Finalized { seq_num })
                        .expect("state_root_requests receiver should never be dropped");
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

    fn metrics(&self) -> &Self::Metrics {
        &()
    }
}
