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

pub mod blob;
pub mod cache;
pub mod common;
#[cfg(feature = "fjall")]
pub mod fjall;
pub mod meta;
pub mod session;

pub use blob::{BlobStore, BlobTable, BlobTableId, BlobWriteOp, InMemoryBlobStore};
#[cfg(feature = "dynamo")]
pub use blob::{DynamoBlobStore, DynamoBlobStoreConfig};
#[cfg(feature = "s3")]
pub use blob::{S3BlobStore, S3BlobStoreConfig, S3Credentials, S3ReadStatsSnapshot};
pub use cache::{
    BlockRegionCache, CacheConfig, CacheField, CachedBlobTable, CachedKvTable,
    CachedScannableTable, REGION_CACHE_FAMILIES,
};
pub use common::Page;
#[cfg(feature = "fjall")]
pub use fjall::{FjallKeyspaceStats, FjallStore, FjallTuning};
#[cfg(feature = "dynamo")]
pub use meta::{
    DynamoCredentials, DynamoMetaReadStatsSnapshot, DynamoMetaStore, DynamoMetaStoreConfig,
    DynamoTableLayout,
};
pub use meta::{
    InMemoryMetaStore, KvTable, MetaStore, MetaWriteOp, ScannableKvTable, ScannableTableId, TableId,
};
pub use session::{SessionFuture, WriteSession};
