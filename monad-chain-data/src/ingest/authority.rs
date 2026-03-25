use crate::{
    error::{Error, Result},
    store::publication::SessionId,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteContinuity {
    Fresh,
    Continuous,
    Reacquired,
}

impl WriteContinuity {
    pub fn requires_recovery(self) -> bool {
        matches!(self, Self::Fresh | Self::Reacquired)
    }
}

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

#[derive(Debug, Default, Clone, Copy)]
pub struct ReadOnlyAuthority;

pub struct ReadOnlyWriteSession<'a> {
    _marker: std::marker::PhantomData<&'a ReadOnlyAuthority>,
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
        Err(Error::ReadOnlyMode(
            "reader-only service cannot acquire write authority",
        ))
    }
}

#[allow(dead_code)]
pub struct LeaseAuthority<P> {
    publication_store: P,
    owner_id: u64,
    session_id: SessionId,
    lease_blocks: u64,
    renew_threshold_blocks: u64,
}

impl<P> LeaseAuthority<P> {
    pub fn new(
        publication_store: P,
        owner_id: u64,
        lease_blocks: u64,
        renew_threshold_blocks: u64,
    ) -> Self {
        let mut session_id = [0u8; 16];
        session_id[..8].copy_from_slice(&owner_id.to_be_bytes());
        Self {
            publication_store,
            owner_id,
            session_id,
            lease_blocks,
            renew_threshold_blocks,
        }
    }
}

pub struct LeaseWriteSession<'a, P> {
    _authority: &'a LeaseAuthority<P>,
}

impl<P> WriteSession for LeaseWriteSession<'_, P>
where
    P: Send + Sync,
{
    fn state(&self) -> AuthorityState {
        AuthorityState {
            indexed_finalized_head: 0,
            continuity: WriteContinuity::Continuous,
        }
    }

    async fn publish(
        self,
        _new_head: u64,
        _observed_upstream_finalized_block: Option<u64>,
    ) -> Result<()> {
        Err(Error::Unsupported(
            "lease authority publish is not implemented in commit 2",
        ))
    }
}

impl<P> WriteAuthority for LeaseAuthority<P>
where
    P: Send + Sync,
{
    type Session<'a>
        = LeaseWriteSession<'a, P>
    where
        Self: 'a;

    async fn begin_write(
        &self,
        _observed_upstream_finalized_block: Option<u64>,
    ) -> Result<Self::Session<'_>> {
        Err(Error::Unsupported(
            "lease authority acquisition is not implemented in commit 2",
        ))
    }
}
