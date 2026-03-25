pub type SessionId = [u8; 16];

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PublicationState {
    pub owner_id: u64,
    pub session_id: SessionId,
    pub indexed_finalized_head: u64,
    pub lease_valid_through_block: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CasOutcome<T> {
    Applied(T),
    Failed { current: Option<T> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FinalizedHeadState {
    pub indexed_finalized_head: u64,
}

pub struct MetaPublicationStore<M> {
    meta_store: M,
}

impl<M> MetaPublicationStore<M> {
    pub fn new(meta_store: M) -> Self {
        Self { meta_store }
    }

    pub fn meta_store(&self) -> &M {
        &self.meta_store
    }
}
