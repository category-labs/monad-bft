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

//! chain-data external payload access over THIS crate's archive stores.
//!
//! External-archive chain-data stores range-read row payloads out of the
//! archive's own block-data storage. For the Mongo and Dynamo backends those
//! formats (`KeyValueDocument` documents, `tx_hash`/`data`/`chunks` items,
//! the `_chunk_{i}` convention) are owned here, so the
//! [`monad_query_primitives::ExternalBlobReader`] implementations live here too —
//! built from the same `[store.archive]` config section chain-data parses,
//! and injected into chain-data's entry points by the embedding binary.
//! (The S3 archive backend has no storage format of its own, so chain-data
//! builds that reader internally.)

use std::sync::Arc;

use aws_config::{retry::RetryConfig, timeout::TimeoutConfig, BehaviorVersion, Region, SdkConfig};
use aws_sdk_s3::config::{Credentials, SharedCredentialsProvider};
use bytes::Bytes;
use eyre::OptionExt;
use futures::future::BoxFuture;
use monad_query_config::ChainDataArchiveBackendConfig;
use monad_query_errors::MonadChainDataError;
use monad_query_primitives::ExternalBlobReader;

use crate::{
    kvstore::{dynamodb::DynamoDBArchive, mongo::MongoDbStorage},
    prelude::*,
};

/// [`ExternalBlobReader`] over an archive block-data store whose byte format
/// this crate owns.
pub enum ArchiveExternalReader {
    Mongo(MongoDbStorage),
    Dynamo(DynamoDBArchive),
}

impl ExternalBlobReader for ArchiveExternalReader {
    fn read_range(
        &self,
        key: &[u8],
        start: usize,
        end_exclusive: usize,
    ) -> BoxFuture<'_, monad_query_errors::Result<Option<Bytes>>> {
        let key = std::str::from_utf8(key).map(str::to_owned);
        Box::pin(async move {
            let key = key.map_err(|_| {
                MonadChainDataError::Decode("external archive key is not valid utf-8")
            })?;
            let result = match self {
                Self::Mongo(store) => store.read_range(&key, start, end_exclusive).await,
                Self::Dynamo(store) => store.read_range(&key, start, end_exclusive).await,
            };
            result.map_err(|e| {
                MonadChainDataError::Backend(format!("archive external read of {key}: {e:#}"))
            })
        })
    }
}

/// Builds the external payload reader for the archive-format backends of a
/// `[store.archive]` config, or `Ok(None)` for backends chain-data builds
/// itself (S3) and for absent config. Pass the result to
/// `run_configured_chain_data_engine_ingest` /
/// `open_configured_chain_data_reader`.
pub async fn build_archive_external_reader(
    config: &Option<ChainDataArchiveBackendConfig>,
) -> Result<Option<Arc<dyn ExternalBlobReader>>> {
    match config {
        Some(ChainDataArchiveBackendConfig::Mongo(mongo)) => {
            let url = mongo
                .url
                .0
                .as_deref()
                .ok_or_eyre("[store.archive] mongo requires url")?;
            let database = mongo
                .database
                .as_deref()
                .ok_or_eyre("[store.archive] mongo requires database")?;
            let store = MongoDbStorage::new_reader(
                url,
                database,
                &mongo.collection,
                mongo.max_pool_size,
                Metrics::none(),
            )
            .await
            .wrap_err("building archive mongo external reader")?;
            Ok(Some(Arc::new(ArchiveExternalReader::Mongo(store))))
        }
        Some(ChainDataArchiveBackendConfig::Dynamo(dynamo)) => {
            let table = dynamo
                .table
                .clone()
                .ok_or_eyre("[store.archive] dynamo requires table")?;
            let sdk_config = dynamo_sdk_config(
                dynamo.endpoint_url.as_deref(),
                dynamo.region.as_deref(),
                dynamo.profile.as_deref(),
                dynamo.access_key_id.0.as_deref(),
                dynamo.secret_access_key.0.as_deref(),
                dynamo.session_token.0.clone(),
            )
            .await;
            let store =
                DynamoDBArchive::new(table, &sdk_config, dynamo.max_concurrency, Metrics::none());
            Ok(Some(Arc::new(ArchiveExternalReader::Dynamo(store))))
        }
        _ => Ok(None),
    }
}

/// SDK config for the dynamo archive reader. Mirrors `ScyllaCliArgs::config`:
/// when an explicit endpoint is set with no credentials and no profile, fall
/// back to placeholder static credentials (Alternator accepts any signature
/// unless authorization is enabled, but the SDK requires *some* credentials).
async fn dynamo_sdk_config(
    endpoint_url: Option<&str>,
    region: Option<&str>,
    profile: Option<&str>,
    access_key_id: Option<&str>,
    secret_access_key: Option<&str>,
    session_token: Option<String>,
) -> SdkConfig {
    let mut loader = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new(region.unwrap_or("us-east-1").to_string()))
        .timeout_config(
            TimeoutConfig::builder()
                .operation_timeout(Duration::from_secs(40))
                .operation_attempt_timeout(Duration::from_secs(10))
                .read_timeout(Duration::from_secs(10))
                .build(),
        )
        .retry_config(RetryConfig::standard().with_max_attempts(3));
    if let Some(profile) = profile {
        loader = loader.profile_name(profile);
    }
    match (access_key_id, secret_access_key) {
        (Some(access_key_id), Some(secret_access_key)) => {
            loader = loader.credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                access_key_id,
                secret_access_key,
                session_token,
                None,
                "chain-data-archive",
            )));
        }
        _ if endpoint_url.is_some() && profile.is_none() => {
            loader = loader.credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                "archive",
                "archive",
                None,
                None,
                "chain-data-archive",
            )));
        }
        _ => {}
    }
    if let Some(endpoint) = endpoint_url {
        loader = loader.endpoint_url(endpoint);
    }
    loader.load().await
}
