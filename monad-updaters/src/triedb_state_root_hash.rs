use std::{
    marker::PhantomData,
    ops::DerefMut,
    path::Path,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use futures::Stream;
use monad_consensus_types::{
    block::{Block, BlockType},
    signature_collection::SignatureCollection,
    state_root_hash::{StateRootHash, StateRootHashInfo},
    validator_data::ValidatorSetData,
};
use monad_crypto::{certificate_signature::CertificateSignatureRecoverable, hasher::Hash};
use monad_executor::Executor;
use monad_executor_glue::{MonadEvent, StateRootHashCommand};
use monad_triedb::Handle as TriedbHandle;
use monad_types::{Epoch, SeqNum};
use tracing::{debug, error, warn};

use crate::state_root_hash::ValidatorSetUpdate;

/// Updater that gets state root hash updates by polling triedb
pub struct StateRootHashTriedbPoll<ST, SCT: SignatureCollection> {
    triedb_recv: tokio::sync::mpsc::Receiver<StateRootHashInfo>,
    seq_num_send: std::sync::mpsc::Sender<SeqNum>,

    // TODO: where will we get this validator set updates
    // validator set updates
    genesis_validator_data: ValidatorSetData<SCT>,
    next_val_data: Option<ValidatorSetUpdate<SCT>>,
    val_set_update_interval: SeqNum,

    waker: Option<Waker>,
    phantom: PhantomData<ST>,
}

impl<ST, SCT: SignatureCollection> StateRootHashTriedbPoll<ST, SCT> {
    pub fn new(
        triedb_path: &Path,
        genesis_validator_data: ValidatorSetData<SCT>,
        val_set_update_interval: SeqNum,
    ) -> Self {
        // TODO: the channel size should be the delay gap
        let (triedb_send, triedb_recv) = tokio::sync::mpsc::channel(10);

        let (seq_num_send, seq_num_recv) = std::sync::mpsc::channel();

        let path = triedb_path.to_path_buf();
        rayon::spawn(move || {
            // FIXME: handle error, maybe retry
            let handle = TriedbHandle::try_new(path.as_path()).unwrap();

            loop {
                let seq_num: SeqNum = seq_num_recv.recv().unwrap(); //FIXME
                'poll_triedb: loop {
                    let result = handle.get_state_root(seq_num.0);
                    debug!("srh = {:?}", result);
                    if let Some(state_root) = result {
                        let state_root = state_root
                            .try_into()
                            .expect("state root from triedb must be 32 Bytes");
                        let s = StateRootHashInfo {
                            state_root_hash: StateRootHash(Hash(state_root)),
                            seq_num,
                        };

                        triedb_send.blocking_send(s).unwrap();
                        break 'poll_triedb;
                    } else {
                        warn!("no state root for blocknum {:?}", seq_num);
                    }
                    std::thread::sleep(std::time::Duration::from_millis(50));
                }
            }
        });

        Self {
            triedb_recv,
            seq_num_send,
            genesis_validator_data,
            next_val_data: None,
            val_set_update_interval,

            waker: None,
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
    type Command = StateRootHashCommand<Block<SCT>, SCT>;

    fn replay(&mut self, mut commands: Vec<Self::Command>) {
        commands.retain(|cmd| match cmd {
            // we match on all commands to be explicit
            StateRootHashCommand::LedgerCommit(..) => true,
            StateRootHashCommand::UpdateValidators(..) => true,
        });
        self.exec(commands)
    }

    fn exec(&mut self, commands: Vec<Self::Command>) {
        let mut wake = false;

        for command in commands {
            match command {
                StateRootHashCommand::LedgerCommit(block) => {
                    let seq_num = block.get_seq_num();
                    if seq_num.is_epoch_end(self.val_set_update_interval) {
                        if self.next_val_data.is_some() {
                            error!("Validator set data is not consumed");
                        }
                        let locked_epoch = seq_num.get_locked_epoch(self.val_set_update_interval);
                        assert_eq!(
                            locked_epoch,
                            seq_num.to_epoch(self.val_set_update_interval) + Epoch(2)
                        );
                        self.next_val_data = Some(ValidatorSetUpdate {
                            epoch: locked_epoch,
                            validator_data: self.genesis_validator_data.clone(),
                        });
                    }
                    self.seq_num_send.send(seq_num); //FIXME
                    wake = true;
                }
                StateRootHashCommand::UpdateValidators(_) => {
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
}
