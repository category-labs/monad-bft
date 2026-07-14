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

//! chain-data external payload access over archive block-data stores: this
//! crate owns the Mongo/Dynamo byte formats, so the
//! [`monad_query_primitives::ExternalBlobReader`] impls live here too.

use std::{sync::Arc, time::Duration};

use aws_config::{retry::RetryConfig, timeout::TimeoutConfig, BehaviorVersion, Region, SdkConfig};
use aws_sdk_dynamodb::Client as DynamoClient;
use aws_sdk_s3::config::{Credentials, SharedCredentialsProvider};
use bytes::Bytes;
use eyre::{Context, OptionExt, Result};
use futures::future::BoxFuture;
use monad_archive::kvstore::mongo::KeyValueDocument;
use monad_query_config::ChainDataArchiveBackendConfig;
use monad_query_errors::QueryError;
use monad_query_primitives::ExternalBlobReader;
use mongodb::Collection;

/// [`ExternalBlobReader`] over an archive block-data store whose byte format
/// this crate owns. Each variant holds its own storage client so it does not
/// depend on monad-archive's (non-range-read-capable) storage structs.
pub enum ArchiveExternalReader {
    Mongo(Collection<KeyValueDocument>),
    Dynamo { client: DynamoClient, table: String },
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
            let key =
                key.map_err(|_| QueryError::Decode("external archive key is not valid utf-8"))?;
            let result = match self {
                Self::Mongo(collection) => {
                    crate::archive_range_read::mongo_read_range(
                        collection,
                        &key,
                        start,
                        end_exclusive,
                    )
                    .await
                }
                Self::Dynamo { client, table } => {
                    crate::archive_range_read::dynamo_read_range(
                        client,
                        table,
                        &key,
                        start,
                        end_exclusive,
                    )
                    .await
                }
            };
            result
                .map_err(|e| QueryError::Backend(format!("archive external read of {key}: {e:#}")))
        })
    }
}

/// Builds the external payload reader for the archive-format backends of a
/// `[store.archive]` config, or `Ok(None)` for backends chain-data builds
/// itself (S3) and for absent config.
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
            let collection = crate::archive_range_read::mongo_reader_collection(
                url,
                database,
                &mongo.collection,
                mongo.max_pool_size,
            )
            .await
            .wrap_err("building archive mongo external reader")?;
            Ok(Some(Arc::new(ArchiveExternalReader::Mongo(collection))))
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
            let client = DynamoClient::new(&sdk_config);
            Ok(Some(Arc::new(ArchiveExternalReader::Dynamo {
                client,
                table,
            })))
        }
        _ => Ok(None),
    }
}

/// SDK config for the dynamo archive reader. With an explicit endpoint and no
/// credentials/profile, fall back to placeholder static credentials — the SDK
/// requires some, though Alternator accepts any signature by default.
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
