#[allow(async_fn_in_trait)]
pub trait MetaStore: Clone + Send + Sync {}

impl<T> MetaStore for T where T: Clone + Send + Sync {}

#[allow(async_fn_in_trait)]
pub trait BlobStore: Send + Sync {}

impl<T> BlobStore for T where T: Send + Sync {}
