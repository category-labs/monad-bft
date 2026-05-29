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

//! Multi-node write authority — the outer coordination ring on top of the
//! CAS-versioned published head.
//!
//! Exactly one process holds a *lease* and may advance `indexed_finalized_head`;
//! standbys observe passively and take over only once the lease expires.
//! Coordination flows entirely through the single CAS-guarded
//! [`PublicationState`] row plus one shared external clock (the latest observed
//! upstream finalized block number). There is no separate consensus layer.
//!
//! Ported from the `finalized-history-query` reference
//! (`src/ingest/authority.rs`), adapted to write through
//! [`PublicationTables`]' version-CAS ([`MetaStoreCas`]) instead of the
//! reference's content-comparing `PublicationStore::compare_and_set`. The
//! cached lease therefore carries the [`CasVersion`] it was last observed at,
//! and every renew/publish CASes against that version; a conflict is
//! discriminated by reloading and comparing owner/session.

use futures::lock::Mutex;

use crate::{
    engine::tables::PublicationTables,
    error::{MonadChainDataError, Result},
    primitives::state::{PublicationState, SessionId},
    store::meta::{CasOutcome, CasVersion, MetaStoreCas},
};

// --- traits ---

/// Whether starting a write continued the same live session or crossed an
/// ownership boundary. `Fresh`/`Reacquired` require writer-side recovery before
/// artifacts are written; `Continuous` skips it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteContinuity {
    /// This process created the first publication state for the service.
    Fresh,
    /// This process continued the same live publication session without an
    /// ownership transition.
    Continuous,
    /// This process had to reacquire publication ownership after a handoff,
    /// lease expiry, or any other continuity break.
    Reacquired,
}

impl WriteContinuity {
    pub fn requires_recovery(self) -> bool {
        matches!(self, Self::Fresh | Self::Reacquired)
    }
}

/// The writer-visible result of `begin_write`: the authoritative published head
/// to sequence from, and whether recovery must run first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthorityState {
    pub indexed_finalized_head: u64,
    pub continuity: WriteContinuity,
}

#[allow(async_fn_in_trait)]
pub trait WriteSession: Send {
    fn state(&self) -> AuthorityState;

    async fn publish(
        self,
        new_head: u64,
        observed_upstream_finalized_block: Option<u64>,
    ) -> Result<()>;
}

#[allow(async_fn_in_trait)]
pub trait WriteAuthority: Send + Sync {
    type Session<'a>: WriteSession + 'a
    where
        Self: 'a;

    async fn begin_write(
        &self,
        observed_upstream_finalized_block: Option<u64>,
    ) -> Result<Self::Session<'_>>;
}

// --- read-only authority ---

/// Authority for a reader-only service: never acquires ownership. Any
/// `begin_write` attempt is a programming error and returns [`ReadOnlyMode`].
///
/// [`ReadOnlyMode`]: MonadChainDataError::ReadOnlyMode
#[derive(Debug, Default, Clone, Copy)]
pub struct ReadOnlyAuthority;

pub struct ReadOnlyWriteSession<'a> {
    _marker: std::marker::PhantomData<&'a ReadOnlyAuthority>,
}

impl ReadOnlyAuthority {
    fn writer_mode_error() -> MonadChainDataError {
        MonadChainDataError::ReadOnlyMode("reader-only service cannot acquire write authority")
    }
}

impl WriteSession for ReadOnlyWriteSession<'_> {
    fn state(&self) -> AuthorityState {
        unreachable!("reader-only authority never yields a write session")
    }

    async fn publish(
        self,
        _new_head: u64,
        _observed_upstream_finalized_block: Option<u64>,
    ) -> Result<()> {
        unreachable!("reader-only authority never yields a write session")
    }
}

impl WriteAuthority for ReadOnlyAuthority {
    type Session<'a>
        = ReadOnlyWriteSession<'a>
    where
        Self: 'a;

    async fn begin_write(
        &self,
        _observed_upstream_finalized_block: Option<u64>,
    ) -> Result<Self::Session<'_>> {
        Err(Self::writer_mode_error())
    }
}

// --- lease authority ---

/// Default lease span, in upstream finalized blocks, when none is configured.
pub const DEFAULT_LEASE_BLOCKS: u64 = 10;

/// A lease the local process believes it holds, along with the [`CasVersion`]
/// of the publication row it was last observed/written at. The version is the
/// CAS precondition for the next renew/publish (D2: version-CAS, not the
/// reference's whole-state content-CAS).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PublicationLease {
    owner_id: u64,
    session_id: SessionId,
    indexed_finalized_head: u64,
    lease_valid_through_block: u64,
    /// CAS version of the row this lease snapshot came from.
    version: CasVersion,
}

impl PublicationLease {
    fn as_state(self) -> PublicationState {
        PublicationState {
            owner_id: self.owner_id,
            session_id: self.session_id,
            indexed_finalized_head: self.indexed_finalized_head,
            lease_valid_through_block: self.lease_valid_through_block,
        }
    }

    fn needs_renewal(
        self,
        observed_upstream_finalized_block: u64,
        renew_threshold_blocks: u64,
    ) -> bool {
        self.lease_valid_through_block
            .saturating_sub(observed_upstream_finalized_block)
            <= renew_threshold_blocks
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LeaseBeginOutcome {
    lease: PublicationLease,
    continuity: WriteContinuity,
}

fn require_observed_finalized_block(observed_upstream_finalized_block: Option<u64>) -> Result<u64> {
    observed_upstream_finalized_block.ok_or(MonadChainDataError::LeaseObservationUnavailable)
}

/// Generates a per-process `session_id`. Two processes (even sharing an
/// `owner_id`) must not collide, so the id mixes the OS pid, a monotonically
/// increasing process startup nonce, the wall-clock at first use, and the
/// `owner_id`. No `rand` dependency is pulled in for this; the combination is
/// collision-resistant across the processes a single deployment runs.
pub fn new_session_id(owner_id: u64) -> SessionId {
    use std::{
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    static NEXT_SESSION_NONCE: AtomicU64 = AtomicU64::new(1);

    let nonce = NEXT_SESSION_NONCE.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id() as u64;
    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);

    let mut session_id = [0u8; 16];
    let high = pid.rotate_left(32) ^ now_nanos;
    let low = owner_id ^ nonce.rotate_left(17) ^ now_nanos.rotate_left(41);
    session_id[..8].copy_from_slice(&high.to_be_bytes());
    session_id[8..].copy_from_slice(&low.to_be_bytes());
    session_id
}

/// The lease-backed write authority for a reader+writer service. Holds the
/// publication CAS table, this node's stable `owner_id`, this process's
/// `session_id`, the lease/renew block windows, and the cached lease the active
/// writer renews and publishes against.
pub struct LeaseAuthority<M: MetaStoreCas> {
    publication: PublicationTables<M>,
    owner_id: u64,
    session_id: SessionId,
    lease_blocks: u64,
    renew_threshold_blocks: u64,
    lease: Mutex<Option<PublicationLease>>,
}

impl<M: MetaStoreCas> LeaseAuthority<M> {
    pub fn new(
        publication: PublicationTables<M>,
        owner_id: u64,
        lease_blocks: u64,
        renew_threshold_blocks: u64,
    ) -> Self {
        Self::with_session(
            publication,
            owner_id,
            new_session_id(owner_id),
            lease_blocks,
            renew_threshold_blocks,
        )
    }

    pub(crate) fn with_session(
        publication: PublicationTables<M>,
        owner_id: u64,
        session_id: SessionId,
        lease_blocks: u64,
        renew_threshold_blocks: u64,
    ) -> Self {
        assert!(lease_blocks > 0, "lease_blocks must be at least 1");
        assert!(
            renew_threshold_blocks < lease_blocks,
            "renew_threshold_blocks must be less than lease_blocks"
        );
        Self {
            publication,
            owner_id,
            session_id,
            lease_blocks,
            renew_threshold_blocks,
            lease: Mutex::new(None),
        }
    }

    fn lease_valid_through_block(&self, observed_upstream_finalized_block: u64) -> u64 {
        observed_upstream_finalized_block.saturating_add(self.lease_blocks - 1)
    }

    fn lease_from(&self, state: PublicationState, version: CasVersion) -> PublicationLease {
        PublicationLease {
            owner_id: state.owner_id,
            session_id: state.session_id,
            indexed_finalized_head: state.indexed_finalized_head,
            lease_valid_through_block: state.lease_valid_through_block,
            version,
        }
    }

    // --- acquisition ---

    /// Acquires ownership from the store with no usable cached lease: creates
    /// the row if absent (`Fresh`), renews in place if we still own a valid
    /// lease (`Continuous`), or takes over an expired/foreign lease
    /// (`Reacquired`). A foreign owner whose lease is still valid yields
    /// [`LeaseStillFresh`]; a lost ownership race yields [`LeaseLost`].
    ///
    /// [`LeaseStillFresh`]: MonadChainDataError::LeaseStillFresh
    /// [`LeaseLost`]: MonadChainDataError::LeaseLost
    async fn acquire_publication_with_session(
        &self,
        observed_upstream_finalized_block: Option<u64>,
    ) -> Result<LeaseBeginOutcome> {
        let observed_upstream_finalized_block =
            require_observed_finalized_block(observed_upstream_finalized_block)?;

        let mut current = match self.publication.load_state().await? {
            Some((version, state)) => self.lease_from(state, version),
            None => {
                let initial = PublicationState {
                    owner_id: self.owner_id,
                    session_id: self.session_id,
                    indexed_finalized_head: 0,
                    lease_valid_through_block: self
                        .lease_valid_through_block(observed_upstream_finalized_block),
                };
                match self.cas_state(None, initial).await? {
                    CasOutcome::Applied { new_version } => {
                        return Ok(LeaseBeginOutcome {
                            lease: self.lease_from(initial, new_version),
                            continuity: WriteContinuity::Fresh,
                        });
                    }
                    // Another process created the row first; reload and fall
                    // through to the acquire/takeover loop.
                    CasOutcome::Conflict { .. } => self.load_lease_or_conflict().await?,
                }
            }
        };

        loop {
            let same_session = current.owner_id == self.owner_id
                && current.session_id == self.session_id
                && observed_upstream_finalized_block <= current.lease_valid_through_block;

            if same_session {
                let extended = self.lease_valid_through_block(observed_upstream_finalized_block);
                if extended <= current.lease_valid_through_block {
                    return Ok(LeaseBeginOutcome {
                        lease: current,
                        continuity: WriteContinuity::Continuous,
                    });
                }
                let next = PublicationState {
                    owner_id: self.owner_id,
                    session_id: self.session_id,
                    indexed_finalized_head: current.indexed_finalized_head,
                    lease_valid_through_block: extended,
                };
                match self.cas_state(Some(current.version), next).await? {
                    CasOutcome::Applied { new_version } => {
                        return Ok(LeaseBeginOutcome {
                            lease: self.lease_from(next, new_version),
                            continuity: WriteContinuity::Continuous,
                        });
                    }
                    CasOutcome::Conflict { .. } => {
                        current = self.load_lease_or_conflict().await?;
                        continue;
                    }
                }
            }

            let same_owner = current.owner_id == self.owner_id;
            if !same_owner && observed_upstream_finalized_block <= current.lease_valid_through_block
            {
                return Err(MonadChainDataError::LeaseStillFresh);
            }

            // Either our own expired lease, or a foreign lease past its
            // validity bound: take over with a fresh validity window, preserving
            // the published head.
            let next = PublicationState {
                owner_id: self.owner_id,
                session_id: self.session_id,
                indexed_finalized_head: current.indexed_finalized_head,
                lease_valid_through_block: self
                    .lease_valid_through_block(observed_upstream_finalized_block),
            };
            match self.cas_state(Some(current.version), next).await? {
                CasOutcome::Applied { new_version } => {
                    return Ok(LeaseBeginOutcome {
                        lease: self.lease_from(next, new_version),
                        continuity: WriteContinuity::Reacquired,
                    });
                }
                CasOutcome::Conflict { .. } => {
                    current = self.load_lease_or_conflict().await?;
                }
            }
        }
    }

    /// Reloads the row after a CAS conflict. The row should exist (we just lost
    /// a race against another writer); its absence means it was removed out from
    /// under us, which we treat as a lost lease.
    async fn load_lease_or_conflict(&self) -> Result<PublicationLease> {
        match self.publication.load_state().await? {
            Some((version, state)) => Ok(self.lease_from(state, version)),
            None => Err(MonadChainDataError::LeaseLost),
        }
    }

    async fn cas_state(
        &self,
        expected: Option<CasVersion>,
        next: PublicationState,
    ) -> Result<CasOutcome> {
        self.publication
            .meta_store()
            .cas_put(
                PublicationTables::<M>::PUBLICATION_STATE_TABLE,
                PublicationTables::<M>::PUBLICATION_STATE_KEY,
                expected,
                bytes::Bytes::from(next.encode()),
            )
            .await
    }

    // --- lifecycle ---

    pub(crate) async fn clear_cached_lease(&self) {
        *self.lease.lock().await = None;
    }

    pub(crate) fn should_invalidate_cached_lease(error: &MonadChainDataError) -> bool {
        matches!(
            error,
            MonadChainDataError::LeaseLost
                | MonadChainDataError::LeaseObservationUnavailable
                | MonadChainDataError::FencedOut { .. }
        )
    }

    /// After an invalidating error, decides whether a same-process retry can
    /// still reuse the live session (the row is still ours / absent) or whether
    /// ownership truly moved away.
    async fn can_retry_after_invalidation(&self) -> Result<bool> {
        Ok(match self.publication.load_state().await? {
            Some((_, state)) => {
                state.owner_id == self.owner_id && state.session_id == self.session_id
            }
            None => true,
        })
    }

    async fn ensure_cached_lease(
        &self,
        observed_upstream_finalized_block: Option<u64>,
    ) -> Result<LeaseBeginOutcome> {
        let mut guard = self.lease.lock().await;
        let outcome = match *guard {
            Some(lease) => LeaseBeginOutcome {
                lease: self
                    .renew_if_needed(lease, observed_upstream_finalized_block)
                    .await?,
                continuity: WriteContinuity::Continuous,
            },
            None => {
                self.acquire_publication_with_session(observed_upstream_finalized_block)
                    .await?
            }
        };
        *guard = Some(outcome.lease);
        Ok(outcome)
    }

    /// Extends `lease_valid_through_block` via CAS when the observed upstream
    /// block has entered the renew window. Owner/session change or an expired
    /// window discovered here means another node took over → [`LeaseLost`].
    ///
    /// [`LeaseLost`]: MonadChainDataError::LeaseLost
    async fn renew_if_needed(
        &self,
        lease: PublicationLease,
        observed_upstream_finalized_block: Option<u64>,
    ) -> Result<PublicationLease> {
        let observed_upstream_finalized_block =
            require_observed_finalized_block(observed_upstream_finalized_block)?;
        if !lease.needs_renewal(
            observed_upstream_finalized_block,
            self.renew_threshold_blocks,
        ) {
            return Ok(lease);
        }

        let mut current = self.load_lease_or_conflict().await?;
        loop {
            if current.owner_id != lease.owner_id || current.session_id != lease.session_id {
                return Err(MonadChainDataError::LeaseLost);
            }
            if observed_upstream_finalized_block > current.lease_valid_through_block {
                return Err(MonadChainDataError::LeaseLost);
            }

            let extended = self.lease_valid_through_block(observed_upstream_finalized_block);
            if extended <= current.lease_valid_through_block {
                return Ok(current);
            }
            let next = PublicationState {
                owner_id: current.owner_id,
                session_id: current.session_id,
                indexed_finalized_head: current.indexed_finalized_head,
                lease_valid_through_block: extended,
            };
            match self.cas_state(Some(current.version), next).await? {
                CasOutcome::Applied { new_version } => {
                    return Ok(self.lease_from(next, new_version));
                }
                CasOutcome::Conflict { .. } => {
                    current = self.load_lease_or_conflict().await?;
                }
            }
        }
    }

    /// Publishes `new_head`: renews the lease if needed, then CASes the row to
    /// the new head against the lease's current version. A conflict whose
    /// reloaded owner/session differs is [`LeaseLost`]; same owner but a moved
    /// version is [`FencedOut`] (a same-owner publication race).
    ///
    /// [`LeaseLost`]: MonadChainDataError::LeaseLost
    /// [`FencedOut`]: MonadChainDataError::FencedOut
    pub async fn publish_current(
        &self,
        new_head: u64,
        observed_upstream_finalized_block: Option<u64>,
    ) -> Result<()> {
        let mut guard = self.lease.lock().await;
        let lease = (*guard).ok_or(MonadChainDataError::LeaseLost)?;
        let lease = self
            .renew_if_needed(lease, observed_upstream_finalized_block)
            .await?;

        let mut next_state = lease.as_state();
        next_state.indexed_finalized_head = new_head;
        match self.cas_state(Some(lease.version), next_state).await? {
            CasOutcome::Applied { new_version } => {
                *guard = Some(self.lease_from(next_state, new_version));
                Ok(())
            }
            CasOutcome::Conflict { .. } => {
                let current_head = match self.publication.load_state().await? {
                    Some((_, state)) => {
                        if state.owner_id != lease.owner_id || state.session_id != lease.session_id
                        {
                            return Err(MonadChainDataError::LeaseLost);
                        }
                        Some(state.indexed_finalized_head)
                    }
                    None => return Err(MonadChainDataError::LeaseLost),
                };
                Err(MonadChainDataError::FencedOut { current_head })
            }
        }
    }
}

pub struct LeaseWriteSession<'a, M: MetaStoreCas> {
    authority: &'a LeaseAuthority<M>,
    state: AuthorityState,
}

impl<M: MetaStoreCas> std::fmt::Debug for LeaseWriteSession<'_, M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaseWriteSession")
            .field("state", &self.state)
            .finish()
    }
}

impl<M: MetaStoreCas> WriteSession for LeaseWriteSession<'_, M> {
    fn state(&self) -> AuthorityState {
        self.state
    }

    async fn publish(
        self,
        new_head: u64,
        observed_upstream_finalized_block: Option<u64>,
    ) -> Result<()> {
        let result = self
            .authority
            .publish_current(new_head, observed_upstream_finalized_block)
            .await;
        if let Some(error) = result.as_ref().err() {
            if LeaseAuthority::<M>::should_invalidate_cached_lease(error) {
                self.authority.clear_cached_lease().await;
            }
        }
        result
    }
}

impl<M: MetaStoreCas> WriteAuthority for LeaseAuthority<M> {
    type Session<'a>
        = LeaseWriteSession<'a, M>
    where
        Self: 'a;

    async fn begin_write(
        &self,
        observed_upstream_finalized_block: Option<u64>,
    ) -> Result<Self::Session<'_>> {
        let begin = match self
            .ensure_cached_lease(observed_upstream_finalized_block)
            .await
        {
            Ok(begin) => begin,
            Err(error) => {
                if Self::should_invalidate_cached_lease(&error) {
                    let retry = self.can_retry_after_invalidation().await?;
                    self.clear_cached_lease().await;
                    if retry {
                        self.ensure_cached_lease(observed_upstream_finalized_block)
                            .await?
                    } else {
                        return Err(error);
                    }
                } else {
                    return Err(error);
                }
            }
        };
        Ok(LeaseWriteSession {
            authority: self,
            state: AuthorityState {
                indexed_finalized_head: begin.lease.indexed_finalized_head,
                continuity: begin.continuity,
            },
        })
    }
}

// --- tests ---

#[cfg(test)]
mod tests {
    use futures::executor::block_on;

    use super::*;
    use crate::store::meta::InMemoryMetaStore;

    fn authority(
        store: &InMemoryMetaStore,
        owner_id: u64,
        session_id: SessionId,
        lease_blocks: u64,
        renew_threshold_blocks: u64,
    ) -> LeaseAuthority<InMemoryMetaStore> {
        LeaseAuthority::with_session(
            PublicationTables::new(store.clone()),
            owner_id,
            session_id,
            lease_blocks,
            renew_threshold_blocks,
        )
    }

    async fn load_state(store: &InMemoryMetaStore) -> PublicationState {
        PublicationTables::new(store.clone())
            .load_state()
            .await
            .expect("load")
            .expect("state present")
            .1
    }

    #[test]
    fn acquire_on_empty_store_is_fresh() {
        block_on(async {
            let store = InMemoryMetaStore::default();
            let authority = authority(&store, 7, [1u8; 16], 50, 10);
            let session = authority.begin_write(Some(100)).await.expect("acquire");
            assert_eq!(session.state().continuity, WriteContinuity::Fresh);
            assert_eq!(session.state().indexed_finalized_head, 0);
            let state = load_state(&store).await;
            assert_eq!(state.owner_id, 7);
            assert_eq!(state.session_id, [1u8; 16]);
            // lease_valid_through_block = observed + lease_blocks - 1
            assert_eq!(state.lease_valid_through_block, 149);
        });
    }

    #[test]
    fn begin_write_does_not_require_recovery_after_cache_invalidation_without_transition() {
        block_on(async {
            let store = InMemoryMetaStore::default();
            let authority = authority(&store, 7, [1u8; 16], 50, 10);

            let first = authority.begin_write(Some(100)).await.expect("acquire");
            assert_eq!(first.state().continuity, WriteContinuity::Fresh);
            // Session holds no guard under D1 option A; just let it fall out of
            // scope before the cache is dropped.
            let _ = first;

            authority.clear_cached_lease().await;

            let second = authority
                .begin_write(Some(120))
                .await
                .expect("reuse same publication session after cache invalidation");
            assert!(
                second.state().continuity == WriteContinuity::Continuous,
                "same-session continuity should not trigger takeover recovery just because the local cache was dropped"
            );
        });
    }

    #[test]
    fn foreign_owner_with_fresh_lease_is_passive() {
        block_on(async {
            let store = InMemoryMetaStore::default();
            // Owner A acquires a 50-block lease at observed=100 → valid through 149.
            let a = authority(&store, 1, [1u8; 16], 50, 10);
            a.begin_write(Some(100)).await.expect("A acquires");

            // Owner B observes a still-valid block (140 <= 149) → must stay passive.
            let b = authority(&store, 2, [2u8; 16], 50, 10);
            let err = b
                .begin_write(Some(140))
                .await
                .expect_err("B must not seize a fresh lease");
            assert!(matches!(err, MonadChainDataError::LeaseStillFresh));

            // Head/owner unchanged.
            let state = load_state(&store).await;
            assert_eq!(state.owner_id, 1);
            assert_eq!(state.indexed_finalized_head, 0);
        });
    }

    #[test]
    fn foreign_owner_takes_over_expired_lease() {
        block_on(async {
            let store = InMemoryMetaStore::default();
            let a = authority(&store, 1, [1u8; 16], 50, 10);
            let sess = a.begin_write(Some(100)).await.expect("A acquires");
            // A publishes head 5 before dying.
            sess.publish(5, Some(100)).await.expect("A publishes");
            let before = load_state(&store).await;
            assert_eq!(before.lease_valid_through_block, 149);

            // B observes a block past the validity bound (150 > 149) → takeover.
            let b = authority(&store, 2, [2u8; 16], 50, 10);
            let session = b.begin_write(Some(150)).await.expect("B takes over");
            assert_eq!(session.state().continuity, WriteContinuity::Reacquired);
            // Head preserved across takeover.
            assert_eq!(session.state().indexed_finalized_head, 5);
            let after = load_state(&store).await;
            assert_eq!(after.owner_id, 2);
            assert_eq!(after.session_id, [2u8; 16]);
            assert_eq!(after.indexed_finalized_head, 5);
            assert_eq!(after.lease_valid_through_block, 199);
        });
    }

    #[test]
    fn renewal_extends_validity_without_changing_session_or_head() {
        block_on(async {
            let store = InMemoryMetaStore::default();
            let authority = authority(&store, 7, [9u8; 16], 50, 10);
            let sess = authority.begin_write(Some(100)).await.expect("acquire");
            sess.publish(3, Some(100)).await.expect("publish");
            let before = load_state(&store).await;
            assert_eq!(before.lease_valid_through_block, 149);

            // Observe into the renew window (149 - 145 = 4 <= 10) → renews.
            let sess = authority.begin_write(Some(145)).await.expect("renew");
            assert_eq!(sess.state().continuity, WriteContinuity::Continuous);
            let after = load_state(&store).await;
            assert_eq!(after.session_id, [9u8; 16]);
            assert_eq!(after.indexed_finalized_head, 3);
            assert_eq!(after.lease_valid_through_block, 194);
        });
    }

    #[test]
    fn missing_observation_fails_closed() {
        block_on(async {
            let store = InMemoryMetaStore::default();
            let authority = authority(&store, 7, [1u8; 16], 50, 10);
            let err = authority
                .begin_write(None)
                .await
                .expect_err("no observation must fail closed");
            assert!(matches!(
                err,
                MonadChainDataError::LeaseObservationUnavailable
            ));
            // Nothing was written.
            assert!(PublicationTables::new(store.clone())
                .load_state()
                .await
                .expect("load")
                .is_none());
        });
    }

    #[test]
    fn two_writer_race_b_takes_over_and_a_publish_is_lease_lost() {
        block_on(async {
            let store = InMemoryMetaStore::default();
            let a = authority(&store, 1, [1u8; 16], 50, 10);
            let a_session = a.begin_write(Some(100)).await.expect("A acquires");

            // B takes over once the lease has expired (observed 150 > 149).
            let b = authority(&store, 2, [2u8; 16], 50, 10);
            let b_session = b.begin_write(Some(150)).await.expect("B takes over");
            assert_eq!(b_session.state().continuity, WriteContinuity::Reacquired);

            // A's publish now finds a foreign owner → LeaseLost, and A clears
            // its cached lease so its next begin_write reacquires.
            let err = a_session
                .publish(7, Some(150))
                .await
                .expect_err("A must be fenced as lease-lost");
            assert!(matches!(err, MonadChainDataError::LeaseLost));
            assert!(a.lease.lock().await.is_none(), "A cleared its cached lease");
        });
    }
}
