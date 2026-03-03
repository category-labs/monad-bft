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
    collections::{btree_map::Entry, BTreeMap, HashSet},
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

use alloy_primitives::Address;
use futures::FutureExt;
use indexmap::IndexSet;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_block_policy::EthValidatedBlock;
use monad_types::{Round, SeqNum};
use monad_validator::signature_collection::SignatureCollection;
use tokio::time::Sleep;
use tracing::{debug, warn};

// Each account lookup takes ~30us so this blocks the thread for at most a touch under 4ms.
const PRELOAD_CHUNK_MAX_ADDRESSES: usize = 128;
const PRELOAD_INTERVAL_MS: u64 = 1;
const COMMITTED_TO_PROPOSAL_SEQNUM_DIFF: SeqNum = SeqNum(2);

pub struct EthTxPoolPreloadEntry {
    // On insertion, we set preload to true. After that, if the SeqNum associated with the entry
    // is no longer in the upcoming predicted leader seqnums then preload is set to false.
    preload: bool,
    pending: IndexSet<Address>,
    done: HashSet<Address>,
}

pub struct EthTxPoolPreloadManager {
    map: BTreeMap<SeqNum, EthTxPoolPreloadEntry>,
    timer: Pin<Box<Sleep>>,
    waker: Option<Waker>,
}

impl Default for EthTxPoolPreloadManager {
    fn default() -> Self {
        Self {
            map: Default::default(),
            timer: Box::pin(tokio::time::sleep(Duration::ZERO)),
            waker: None,
        }
    }
}

impl EthTxPoolPreloadManager {
    pub fn enter_round(
        &mut self,
        round: Round,
        last_commit_seqnum: SeqNum,
        upcoming_leader_rounds: Vec<Round>,
        generate_requests: impl FnOnce() -> Vec<Address>,
    ) {
        self.map = self.map.split_off(&(last_commit_seqnum + SeqNum(1)));

        let upcoming_predicted_proposal_seqnums = upcoming_leader_rounds
            .iter()
            .filter_map(|leader_round| {
                let round_diff = leader_round.0.checked_sub(round.0)?;

                Some(last_commit_seqnum + COMMITTED_TO_PROPOSAL_SEQNUM_DIFF + SeqNum(round_diff))
            })
            .collect::<HashSet<SeqNum>>();

        self.map.retain(|predicted_proposal_seqnum, entry| {
            // We recreate the pending sets from the txpool snapshot if we're a leader in the
            // current round or the next.
            entry.pending.clear();

            // If the last round advances as a result of a TC, then the round number increments
            // while the seqnum does not. In this case, some of the previously predicted proposal
            // seqnums are no longer correct. These entries should be invalidated to avoid
            // unnecessary preloading.
            if !upcoming_predicted_proposal_seqnums.contains(predicted_proposal_seqnum) {
                if entry.done.is_empty() {
                    return false;
                }

                entry.preload = false;
            }

            true
        });

        let preload_round = upcoming_leader_rounds.contains(&round);
        let preload_next_round = upcoming_leader_rounds.contains(&(round + Round(1)));

        if !(preload_round || preload_next_round) {
            return;
        }

        if preload_round {
            let round_predicted_proposal_seqnum =
                last_commit_seqnum + COMMITTED_TO_PROPOSAL_SEQNUM_DIFF;

            self.map
                .entry(round_predicted_proposal_seqnum)
                .or_insert(EthTxPoolPreloadEntry {
                    preload: true,
                    pending: IndexSet::default(),
                    done: HashSet::default(),
                });
        }

        if preload_next_round {
            let next_round_predicted_proposal_seqnum =
                last_commit_seqnum + COMMITTED_TO_PROPOSAL_SEQNUM_DIFF + SeqNum(1);

            self.map
                .entry(next_round_predicted_proposal_seqnum)
                .and_modify(|entry| {
                    // We enable preload for the next round, which remains enabled until we receive a
                    // FetchProposal event for that seqnum and it gets disabled or the entry is
                    // removed.
                    entry.preload = true;
                })
                .or_insert(EthTxPoolPreloadEntry {
                    preload: true,
                    pending: IndexSet::default(),
                    done: HashSet::default(),
                });
        }

        let requests = generate_requests();

        self.add_requests(requests.iter());

        for (predicted_proposal_seqnum, entry) in self.map.iter() {
            debug!(
                ?predicted_proposal_seqnum,
                preload = entry.preload,
                pending = entry.pending.len(),
                done = entry.done.len(),
                upcoming_leader_rounds = ?upcoming_leader_rounds,
                upcoming_predicted_proposal_seqnums =?upcoming_predicted_proposal_seqnums,
                "txpool executor preload manager state"
            );
        }
    }

    pub fn add_requests<'a>(&mut self, requests: impl Iterator<Item = &'a Address> + Clone) {
        for entry in self.map.values_mut() {
            Self::_add_requests(entry, requests.clone(), &mut self.waker);
        }
    }

    fn _add_requests<'a>(
        entry: &mut EthTxPoolPreloadEntry,
        requests: impl Iterator<Item = &'a Address>,
        waker: &mut Option<Waker>,
    ) {
        if !entry.preload {
            return;
        }

        for address in requests {
            if entry.done.contains(address) {
                continue;
            }

            entry.pending.insert(*address);
        }

        if let Some(waker) = waker.take() {
            waker.wake();
        }
    }

    pub fn update_committed_block<ST, SCT>(&mut self, committed_block: &EthValidatedBlock<ST, SCT>)
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    {
        self.map = self
            .map
            .split_off(&(committed_block.get_seq_num() + SeqNum(1)))
    }

    pub fn update_on_create_proposal(&mut self, create_proposal_seqnum: SeqNum) {
        for (_, entry) in self.map.range_mut(..=create_proposal_seqnum) {
            entry.preload = false;
            entry.pending.clear();
        }
    }

    pub fn poll_requests(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<(SeqNum, IndexSet<Address>)> {
        if let Some(waker) = self.waker.as_mut() {
            waker.clone_from(cx.waker());
            return Poll::Pending;
        }

        let Poll::Ready(_) = self.timer.poll_unpin(cx) else {
            return Poll::Pending;
        };

        for (predicted_proposal_seqnum, entry) in self.map.iter_mut() {
            if !entry.preload || entry.pending.is_empty() {
                continue;
            }

            if let Some(at) = entry.pending.len().checked_sub(PRELOAD_CHUNK_MAX_ADDRESSES) {
                if at > 0 {
                    return Poll::Ready((*predicted_proposal_seqnum, entry.pending.split_off(at)));
                }
            }

            return Poll::Ready((
                *predicted_proposal_seqnum,
                std::mem::take(&mut entry.pending),
            ));
        }

        self.waker = Some(cx.waker().clone());
        Poll::Pending
    }

    pub fn complete_polled_requests(
        &mut self,
        predicted_proposal_seqnum: SeqNum,
        requests: impl Iterator<Item = Address>,
    ) {
        self.timer.set(tokio::time::sleep(Duration::from_millis(
            PRELOAD_INTERVAL_MS,
        )));

        let Entry::Occupied(mut entry) = self.map.entry(predicted_proposal_seqnum) else {
            warn!(
                ?predicted_proposal_seqnum,
                "txpool executor preload manager received complete request for unknown seqnum"
            );
            return;
        };

        entry.get_mut().done.extend(requests);

        for (predicted_proposal_seqnum, entry) in self.map.iter() {
            debug!(
                ?predicted_proposal_seqnum,
                preload = entry.preload,
                pending = entry.pending.len(),
                done = entry.done.len(),
                "txpool executor preload manager state after completed requests"
            );
        }
    }
}

#[cfg(test)]
mod test {
    use std::{
        pin::{pin, Pin},
        task::{Context, Poll},
        time::Duration,
    };

    use alloy_primitives::Address;
    use futures::task::noop_waker_ref;
    use itertools::Itertools;
    use monad_types::{Round, SeqNum};

    use super::EthTxPoolPreloadManager;
    use crate::preload::{PRELOAD_CHUNK_MAX_ADDRESSES, PRELOAD_INTERVAL_MS};

    fn setup<'a>() -> (EthTxPoolPreloadManager, Context<'a>) {
        let mut preload_manager = EthTxPoolPreloadManager::default();

        assert!(preload_manager.map.is_empty());
        preload_manager.enter_round(Round(2), SeqNum(0), vec![Round(2)], Vec::default);
        assert_eq!(preload_manager.map.len(), 1);

        (preload_manager, Context::from_waker(noop_waker_ref()))
    }

    async fn assert_pending_now_and_forever(
        mut preload_manager: Pin<&mut EthTxPoolPreloadManager>,
        mut cx: Context<'_>,
    ) {
        assert_eq!(
            preload_manager.as_mut().poll_requests(&mut cx),
            Poll::Pending
        );

        tokio::time::advance(Duration::from_secs(24 * 60 * 60)).await;

        assert_eq!(
            preload_manager.as_mut().poll_requests(&mut cx),
            Poll::Pending
        );
        assert!(preload_manager.waker.is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn test_poll_none() {
        let (preload_manager, cx) = setup();
        let preload_manager = pin!(preload_manager);

        assert_pending_now_and_forever(preload_manager, cx).await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_add() {
        let (preload_manager, _) = setup();
        let mut preload_manager = pin!(preload_manager);

        preload_manager.add_requests([&Address::default()].into_iter());

        let map_entry = preload_manager.map.get(&SeqNum(2)).unwrap();

        assert!(map_entry.done.is_empty());
        assert!(map_entry.preload);
        assert_eq!(map_entry.pending.len(), 1);
        assert!(map_entry.pending.contains(&Address::default()));
    }

    #[tokio::test(start_paused = true)]
    async fn test_simple() {
        let (preload_manager, mut cx) = setup();
        let mut preload_manager = pin!(preload_manager);

        preload_manager.add_requests([&Address::default()].into_iter());

        let Poll::Ready((seqnum, accounts)) = preload_manager.as_mut().poll_requests(&mut cx)
        else {
            panic!("Expected poll_requests to produce pending account");
        };

        assert_eq!(seqnum, SeqNum(2));
        assert_eq!(accounts.len(), 1);
        assert!(accounts.contains(&Address::default()));

        preload_manager
            .as_mut()
            .complete_polled_requests(SeqNum(2), accounts.into_iter());

        assert!(preload_manager.waker.is_none());

        tokio::time::advance(
            Duration::from_millis(PRELOAD_INTERVAL_MS)
                .checked_sub(Duration::from_micros(1))
                .unwrap(),
        )
        .await;
        assert!(preload_manager.as_mut().poll_requests(&mut cx).is_pending());
        assert!(preload_manager.waker.is_none());

        tokio::time::advance(Duration::from_micros(1)).await;

        assert_pending_now_and_forever(preload_manager, cx).await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_subsequent() {
        let (preload_manager, mut cx) = setup();
        let mut preload_manager = pin!(preload_manager);

        let addresses = (0..PRELOAD_CHUNK_MAX_ADDRESSES + 1)
            .map(|x| {
                let mut bytes = [0u8; 20];
                bytes[12..].copy_from_slice(&x.to_be_bytes());
                Address::new(bytes)
            })
            .collect_vec();

        preload_manager.add_requests(addresses.iter());

        let Poll::Ready((seqnum, accounts)) = preload_manager.as_mut().poll_requests(&mut cx)
        else {
            panic!("Expected poll_requests to produce pending account");
        };

        assert_eq!(seqnum, SeqNum(2));
        assert_eq!(accounts.len(), PRELOAD_CHUNK_MAX_ADDRESSES);
        for address in addresses.iter().skip(1) {
            assert!(accounts.contains(address));
        }

        preload_manager
            .as_mut()
            .complete_polled_requests(SeqNum(2), accounts.into_iter());

        assert!(preload_manager.waker.is_none());

        tokio::time::advance(
            Duration::from_millis(PRELOAD_INTERVAL_MS)
                .checked_sub(Duration::from_micros(1))
                .unwrap(),
        )
        .await;
        assert!(preload_manager.as_mut().poll_requests(&mut cx).is_pending());
        assert!(preload_manager.waker.is_none());

        tokio::time::advance(Duration::from_micros(1)).await;

        let Poll::Ready((seqnum, accounts)) = preload_manager.as_mut().poll_requests(&mut cx)
        else {
            panic!("Expected poll_requests to produce pending account");
        };

        assert_eq!(seqnum, SeqNum(2));
        assert_eq!(accounts.len(), 1);
        assert!(accounts.contains(addresses.first().unwrap()));

        preload_manager
            .as_mut()
            .complete_polled_requests(SeqNum(2), accounts.into_iter());

        assert!(preload_manager.waker.is_none());

        assert_pending_now_and_forever(preload_manager, cx).await;
    }
}
