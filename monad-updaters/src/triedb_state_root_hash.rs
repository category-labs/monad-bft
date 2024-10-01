use std::{
    marker::PhantomData,
    ops::DerefMut,
    path::Path,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

use futures::Stream;
use monad_consensus_types::{
    signature_collection::SignatureCollection,
    state_root_hash::{StateRootHash, StateRootHashInfo},
    validator_data::ValidatorSetData,
};
use monad_crypto::{certificate_signature::CertificateSignatureRecoverable, hasher::Hash};
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{MonadEvent, StateRootHashCommand};
use monad_triedb::TriedbHandle;
use monad_types::{Epoch, SeqNum};
use tracing::{debug, error, warn};

use crate::state_root_hash::ValidatorSetUpdate;

/// Updater that gets state root hash updates by polling triedb
pub struct StateRootHashTriedbPoll<ST, SCT: SignatureCollection> {
    triedb_recv: tokio::sync::mpsc::UnboundedReceiver<StateRootHashInfo>,
    seq_num_send: std::sync::mpsc::Sender<SeqNum>,
    cancel_below: Arc<Mutex<SeqNum>>,

    // TODO: where will we get this validator set updates
    // validator set updates
    genesis_validator_data: ValidatorSetData<SCT>,
    // validator set updates from control panel
    last_val_data: Option<ValidatorSetUpdate<SCT>>,
    next_val_data: Option<ValidatorSetUpdate<SCT>>,
    val_set_update_interval: SeqNum,

    waker: Option<Waker>,
    metrics: ExecutorMetrics,
    phantom: PhantomData<ST>,
}

impl<ST, SCT: SignatureCollection> StateRootHashTriedbPoll<ST, SCT> {
    pub fn new(
        triedb_path: &Path,
        genesis_validator_data: ValidatorSetData<SCT>,
        val_set_update_interval: SeqNum,
    ) -> Self {
        let (triedb_send, triedb_recv) = tokio::sync::mpsc::unbounded_channel();

        let (seq_num_send, seq_num_recv) = std::sync::mpsc::channel();
        let cancel_below = Arc::new(Mutex::new(SeqNum(0)));
        let cancel_below_clone = cancel_below.clone();

        let path = triedb_path.to_path_buf();
        rayon::spawn(move || {
            // FIXME: handle error, maybe retry
            let handle = TriedbHandle::try_new(path.as_path()).unwrap();

            loop {
                let seq_num: SeqNum = seq_num_recv.recv().unwrap(); //FIXME
                let mut num_tries = 0_usize;
                'poll_triedb: loop {
                    if seq_num < *cancel_below_clone.lock().unwrap() {
                        break 'poll_triedb;
                    }
                    num_tries += 1;
                    let result = handle.get_state_root(seq_num.0);
                    debug!(?seq_num, ?result, "polled state_root_hash");
                    if let Some(state_root) = result {
                        let state_root = state_root
                            .try_into()
                            .expect("state root from triedb must be 32 Bytes");
                        let s = StateRootHashInfo {
                            state_root_hash: StateRootHash(Hash(state_root)),
                            seq_num,
                        };

                        triedb_send.send(s).unwrap();
                        break 'poll_triedb;
                    }

                    if num_tries > 1 {
                        warn!(?seq_num, ?num_tries, "no state root");
                    } else {
                        debug!(?seq_num, ?num_tries, "no state root");
                    }
                    std::thread::sleep(std::time::Duration::from_millis(50));
                }
            }
        });

        Self {
            triedb_recv,
            cancel_below,
            seq_num_send,
            genesis_validator_data,
            last_val_data: None,
            next_val_data: None,
            val_set_update_interval,

            waker: None,
            metrics: Default::default(),
            phantom: PhantomData,
        }
    }
}

impl<ST, SCT> Stream for StateRootHashTriedbPoll<ST, SCT>
where
    Self: Unpin,
    ST: CertificateSignatureRecoverable + Unpin,
    SCT: SignatureCollection + Unpin,
{
    type Item = MonadEvent<ST, SCT>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        if this.waker.is_none() {
            this.waker = Some(cx.waker().clone());
        }

        if let Some(next_val_data) = this.next_val_data.take() {
            return Poll::Ready(Some(MonadEvent::ValidatorEvent(
                monad_executor_glue::ValidatorEvent::<SCT>::UpdateValidators((
                    next_val_data.validator_data,
                    next_val_data.epoch,
                )),
            )));
        }

        let s = this.triedb_recv.poll_recv(cx);
        s.map(|s| {
            s.map(|info| {
                MonadEvent::AsyncStateVerifyEvent(
                    monad_executor_glue::AsyncStateVerifyEvent::LocalStateRoot(info),
                )
            })
        })
    }
}

impl<ST, SCT> Executor for StateRootHashTriedbPoll<ST, SCT>
where
    SCT: SignatureCollection,
{
    type Command = StateRootHashCommand<SCT>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        let mut wake = false;

        for command in commands {
            match command {
                StateRootHashCommand::CancelBelow(seq_num) => {
                    *self.cancel_below.lock().unwrap() = seq_num;
                }
                StateRootHashCommand::Request(seq_num) => {
                    if seq_num.is_epoch_end(self.val_set_update_interval) {
                        if self.next_val_data.is_some() {
                            error!("Validator set data is not consumed");
                        }
                        let locked_epoch = seq_num.get_locked_epoch(self.val_set_update_interval);
                        assert_eq!(
                            locked_epoch,
                            seq_num.to_epoch(self.val_set_update_interval) + Epoch(2)
                        );
                        let next_validator_data = self
                            .last_val_data
                            .as_ref()
                            .and_then(|v| {
                                if locked_epoch >= v.epoch {
                                    debug!(locked_epoch = %locked_epoch.0, last_val_data_epoch = %v.epoch.0, num_validators = %v.validator_data.0.len(), "last validator update epoch matched locked epoch");
                                    Some(ValidatorSetUpdate {
                                        epoch: locked_epoch,
                                        validator_data: v.validator_data.clone(),
                                    })
                                } else {
                                    // FIXME review this... the unwrap_or_else below here will run
                                    // if this branch gets hit.
                                    //
                                    // This all should disappear post staking module so fine for
                                    // now
                                    debug!(locked_epoch = %locked_epoch.0, last_val_data_epoch = %v.epoch.0, num_validators = %v.validator_data.0.len(), "last validator update epoch did not match matched locked epoch");
                                    None
                                }
                            })
                            .unwrap_or_else(|| {
                                debug!(locked_epoch = %locked_epoch.0, num_validators = %self.genesis_validator_data.0.len(), "re-using genesis validator set");
                                ValidatorSetUpdate {
                                    epoch: locked_epoch,
                                    validator_data: self.genesis_validator_data.clone(),
                                }
                            });

                        self.next_val_data = Some(next_validator_data);
                    }
                    self.seq_num_send
                        .send(seq_num)
                        .expect("seq_num receiver should never be dropped");
                    wake = true;
                }
                StateRootHashCommand::UpdateValidators((validator_data, epoch)) => {
                    debug!(num_validators = ?validator_data.0.len(), epoch = %epoch.0, "UpdateValidators");
                    self.last_val_data = Some(ValidatorSetUpdate {
                        epoch,
                        validator_data,
                    });
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
