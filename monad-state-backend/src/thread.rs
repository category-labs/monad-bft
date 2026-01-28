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
    sync::{mpsc, Arc},
};

use alloy_primitives::Address;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_types::{EthAccount, EthHeader};
use monad_types::{BlockId, Epoch, SeqNum, Stake};
use monad_validator::signature_collection::{SignatureCollection, SignatureCollectionPubKeyType};
use tracing::warn;

use crate::{ReserveBalanceState, StateBackend, StateBackendError};

// Since the StateBackendThreadClient is synchronous, it will only allow one inflight request per
// sync context so a value of 16 allows 16 threads to simulatneously make state backend requests.
const MAX_INFLIGHT_REQUESTS: usize = 16;

enum StateBackendThreadRequest<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    GetAccountStatuses {
        block_id: BlockId,
        seq_num: SeqNum,
        is_finalized: bool,
        addresses: Vec<Address>,
        tx: mpsc::SyncSender<Result<Vec<Option<EthAccount>>, StateBackendError>>,
    },
    GetExecutionResult {
        block_id: BlockId,
        seq_num: SeqNum,
        is_finalized: bool,
        tx: mpsc::SyncSender<Result<EthHeader, StateBackendError>>,
    },
    GetReserveBalanceStates {
        block_id: BlockId,
        seq_num: SeqNum,
        is_finalized: bool,
        addresses: Vec<Address>,
        tx: mpsc::SyncSender<Result<Vec<Option<ReserveBalanceState>>, StateBackendError>>,
    },
    RawReadEarliestFinalizedBlock {
        tx: mpsc::SyncSender<Option<SeqNum>>,
    },
    RawReadLatestFinalizedBlock {
        tx: mpsc::SyncSender<Option<SeqNum>>,
    },
    ReadValidatorSetAtBlock {
        block_num: SeqNum,
        requested_epoch: Epoch,
        tx: mpsc::SyncSender<
            Vec<(
                CertificateSignaturePubKey<ST>,
                SignatureCollectionPubKeyType<SCT>,
                Stake,
            )>,
        >,
    },
    TotalDbLookups {
        tx: mpsc::SyncSender<u64>,
    },
}

#[derive(Clone)]
pub struct StateBackendThreadClient<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    handle: Arc<std::thread::JoinHandle<()>>,
    request_tx: mpsc::SyncSender<StateBackendThreadRequest<ST, SCT>>,
}

impl<ST, SCT> StateBackendThreadClient<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    pub fn new<SBT>(state_backend: impl FnOnce() -> SBT + Send + 'static) -> Self
    where
        SBT: StateBackend<ST, SCT>,
    {
        let (request_tx, request_rx) = mpsc::sync_channel(MAX_INFLIGHT_REQUESTS);

        let handle = Arc::new(std::thread::spawn(move || {
            StateBackendThread::new(state_backend, request_rx).run()
        }));

        Self { handle, request_tx }
    }

    fn send_and_recv_request<T>(
        &self,
        request: impl FnOnce(mpsc::SyncSender<T>) -> StateBackendThreadRequest<ST, SCT>,
    ) -> T {
        if self.handle.is_finished() {
            panic!("StateBackendThread terminated!");
        }

        let (tx, rx) = mpsc::sync_channel(0);

        self.request_tx
            .send(request(tx))
            .expect("StateBackendThread is alive");

        rx.recv().expect("StateBackendThread sends response")
    }
}

impl<ST, SCT> StateBackend<ST, SCT> for StateBackendThreadClient<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn get_account_statuses<'a>(
        &self,
        block_id: &BlockId,
        seq_num: &SeqNum,
        is_finalized: bool,
        addresses: impl Iterator<Item = &'a Address>,
    ) -> Result<Vec<Option<EthAccount>>, StateBackendError> {
        self.send_and_recv_request(|tx| StateBackendThreadRequest::GetAccountStatuses {
            block_id: block_id.to_owned(),
            seq_num: seq_num.to_owned(),
            is_finalized,
            addresses: addresses.cloned().collect(),
            tx,
        })
    }

    fn get_execution_result(
        &self,
        block_id: &BlockId,
        seq_num: &SeqNum,
        is_finalized: bool,
    ) -> Result<EthHeader, StateBackendError> {
        self.send_and_recv_request(|tx| StateBackendThreadRequest::GetExecutionResult {
            block_id: block_id.to_owned(),
            seq_num: seq_num.to_owned(),
            is_finalized,
            tx,
        })
    }

    fn get_reserve_balance_states<'a>(
        &self,
        block_id: &BlockId,
        seq_num: &SeqNum,
        is_finalized: bool,
        addresses: impl Iterator<Item = &'a Address>,
    ) -> Result<Vec<Option<ReserveBalanceState>>, StateBackendError> {
        self.send_and_recv_request(|tx| StateBackendThreadRequest::GetReserveBalanceStates {
            block_id: block_id.to_owned(),
            seq_num: seq_num.to_owned(),
            is_finalized,
            addresses: addresses.cloned().collect(),
            tx,
        })
    }

    fn raw_read_earliest_finalized_block(&self) -> Option<SeqNum> {
        self.send_and_recv_request(
            |tx| StateBackendThreadRequest::RawReadEarliestFinalizedBlock { tx },
        )
    }

    fn raw_read_latest_finalized_block(&self) -> Option<SeqNum> {
        self.send_and_recv_request(
            |tx| StateBackendThreadRequest::RawReadLatestFinalizedBlock { tx },
        )
    }

    fn read_valset_at_block(
        &self,
        block_num: SeqNum,
        requested_epoch: Epoch,
    ) -> Vec<(SCT::NodeIdPubKey, SignatureCollectionPubKeyType<SCT>, Stake)> {
        self.send_and_recv_request(|tx| StateBackendThreadRequest::ReadValidatorSetAtBlock {
            block_num,
            requested_epoch,
            tx,
        })
    }

    fn total_db_lookups(&self) -> u64 {
        self.send_and_recv_request(|tx| StateBackendThreadRequest::TotalDbLookups { tx })
    }
}

struct StateBackendThread<ST, SCT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT>,
{
    state_backend: SBT,
    request_rx: mpsc::Receiver<StateBackendThreadRequest<ST, SCT>>,

    _phantom: PhantomData<(ST, SCT)>,
}

impl<ST, SCT, SBT> StateBackendThread<ST, SCT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT>,
{
    fn new(
        state_backend: impl FnOnce() -> SBT + Send + 'static,
        request_rx: mpsc::Receiver<StateBackendThreadRequest<ST, SCT>>,
    ) -> Self {
        let state_backend = state_backend();

        Self {
            state_backend,
            request_rx,

            _phantom: PhantomData,
        }
    }

    fn run(self) {
        let Self {
            state_backend,
            request_rx,
            ..
        } = self;

        for request in request_rx.iter() {
            match request {
                StateBackendThreadRequest::GetAccountStatuses {
                    block_id,
                    seq_num,
                    is_finalized,
                    addresses,
                    tx,
                } => {
                    tx.send(state_backend.get_account_statuses(
                        &block_id,
                        &seq_num,
                        is_finalized,
                        addresses.iter(),
                    ))
                    .expect("StateBackendThreadClient is alive");
                }
                StateBackendThreadRequest::GetExecutionResult {
                    block_id,
                    seq_num,
                    is_finalized,
                    tx,
                } => {
                    tx.send(state_backend.get_execution_result(&block_id, &seq_num, is_finalized))
                        .expect("StateBackendThreadClient is alive");
                }
                StateBackendThreadRequest::GetReserveBalanceStates {
                    block_id,
                    seq_num,
                    is_finalized,
                    addresses,
                    tx,
                } => {
                    tx.send(state_backend.get_reserve_balance_states(
                        &block_id,
                        &seq_num,
                        is_finalized,
                        addresses.iter(),
                    ))
                    .expect("StateBackendThreadClient is alive");
                }
                StateBackendThreadRequest::RawReadEarliestFinalizedBlock { tx } => {
                    tx.send(state_backend.raw_read_earliest_finalized_block())
                        .expect("StateBackendThreadClient is alive");
                }
                StateBackendThreadRequest::RawReadLatestFinalizedBlock { tx } => {
                    tx.send(state_backend.raw_read_latest_finalized_block())
                        .expect("StateBackendThreadClient is alive");
                }
                StateBackendThreadRequest::ReadValidatorSetAtBlock {
                    block_num,
                    requested_epoch,
                    tx,
                } => {
                    tx.send(state_backend.read_valset_at_block(block_num, requested_epoch))
                        .expect("StateBackendThreadClient is alive");
                }
                StateBackendThreadRequest::TotalDbLookups { tx } => {
                    tx.send(state_backend.total_db_lookups())
                        .expect("StateBackendThreadClient is alive");
                }
            }
        }

        warn!("StateBackendThread terminating");
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use monad_crypto::NopSignature;
    use monad_multi_sig::MultiSig;
    use monad_types::{Balance, SeqNum, GENESIS_BLOCK_ID, GENESIS_SEQ_NUM};

    use crate::{InMemoryStateInner, StateBackend, StateBackendThreadClient};

    #[test]
    fn all_requests() {
        let client = StateBackendThreadClient::new(|| {
            InMemoryStateInner::<NopSignature, MultiSig<NopSignature>>::genesis(
                Balance::MAX,
                SeqNum(4),
            )
        });

        {
            let get_account_statuses = client
                .get_account_statuses(&GENESIS_BLOCK_ID, &GENESIS_SEQ_NUM, true, [].into_iter())
                .unwrap();

            assert_eq!(get_account_statuses.len(), 0);
        }

        {
            let earliest_finalized_block = client.raw_read_earliest_finalized_block().unwrap();

            assert_eq!(earliest_finalized_block, GENESIS_SEQ_NUM);
        }

        {
            let latest_finalized_block = client.raw_read_latest_finalized_block().unwrap();

            assert_eq!(latest_finalized_block, GENESIS_SEQ_NUM);
        }

        {
            let total_db_lookups = client.total_db_lookups();

            assert_eq!(total_db_lookups, 0);
        }
    }

    #[test]
    fn shutdown() {
        let client = StateBackendThreadClient::new(|| {
            InMemoryStateInner::<NopSignature, MultiSig<NopSignature>>::genesis(
                Balance::MAX,
                SeqNum(4),
            )
        });

        let handle = client.handle.clone();

        drop(client);

        std::thread::sleep(Duration::from_millis(10));

        assert!(handle.is_finished());
    }
}
