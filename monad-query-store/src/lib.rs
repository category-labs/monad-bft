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

#[cfg(any(feature = "dynamo", feature = "s3"))]
pub(crate) mod aws;
pub mod blob;
pub mod cache;
#[cfg(feature = "dynamo")]
pub mod dynamo_common;
pub mod meta;
#[cfg(any(feature = "dynamo", feature = "s3"))]
pub(crate) mod read_stats;

pub use blob::{BlobStore, BlobTable, BlobTableId, BlobWriteOp, InMemoryBlobStore, NullBlobStore};
#[cfg(feature = "dynamo")]
pub use blob::{DynamoBlobStore, DynamoBlobStoreConfig};
#[cfg(feature = "s3")]
pub use blob::{
    S3BlobStore, S3BlobStoreConfig, S3Credentials, S3ExternalBlobReader, S3ReadStatsSnapshot,
};
pub use cache::{CacheConfig, CachedKvTable, CachedScannableKvTable};
#[cfg(feature = "dynamo")]
pub use meta::{
    DynamoCredentials, DynamoMetaReadStatsSnapshot, DynamoMetaStore, DynamoMetaStoreConfig,
    DynamoTableLayout,
};
pub use meta::{
    InMemoryMetaStore, KvTable, MetaStore, MetaWriteOp, ScannableKvTable, ScannableTableId, TableId,
};
#[cfg(feature = "mongo")]
pub use meta::{MongoMetaStore, MongoMetaStoreConfig};
pub use monad_query_errors::QueryError;
