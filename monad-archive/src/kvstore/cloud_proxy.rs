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

use std::time::Duration;

use alloy_primitives::TxHash;
use base64::Engine;
use bytes::Bytes;
use eyre::Result;
use reqwest::{header::HeaderValue, StatusCode};
use url::Url;

use super::{KVStoreType, MetricsResultExt, ObjectMeta};
use crate::prelude::*;

#[derive(Clone)]
pub struct CloudProxyReader(Arc<CloudProxyReaderInner>);

struct CloudProxyReaderInner {
    pub client: reqwest::Client,
    pub url: Url,
    pub table: String,
    pub metrics: Metrics,
}

impl CloudProxyReader {
    pub fn new(api_key: &str, url: Url, table: String) -> Result<Self> {
        Self::new_with_metrics(api_key, url, table, Metrics::none())
    }

    pub fn new_with_metrics(
        api_key: &str,
        url: Url,
        table: String,
        metrics: Metrics,
    ) -> Result<Self> {
        let mut headers = reqwest::header::HeaderMap::with_capacity(1);
        headers.insert("x-api-key", HeaderValue::from_str(api_key)?);
        let client = reqwest::ClientBuilder::new()
            .default_headers(headers)
            .timeout(Duration::from_secs(2))
            .build()?;
        Ok(Self(Arc::new(CloudProxyReaderInner {
            client,
            url,
            table,
            metrics,
        })))
    }

    fn build_url(&self, key: &str) -> Url {
        let mut url = self.0.url.clone();
        url.query_pairs_mut()
            .append_pair("txhash", key)
            .append_pair("table", &self.0.table);
        url
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct ProxyResponse {
    tx_hash: TxHash,
    data: String,
    /// Base64-encoded SHA256 of the payload; absent on older proxy responses.
    #[serde(default)]
    checksum: Option<String>,
}

impl KVReader for CloudProxyReader {
    async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        let start = Instant::now();
        let result = async {
            let url = self.build_url(key);
            let resp = self.0.client.get(url).send().await?;
            let status = resp.status();
            if status == StatusCode::NOT_FOUND {
                return Ok(None);
            }
            if !status.is_success() {
                bail!("cloud proxy get failed with status: {status}");
            }
            let resp: ProxyResponse = resp.json().await?;
            let bytes = base64::prelude::BASE64_STANDARD.decode(resp.data)?;

            Ok(Some(bytes.into()))
        }
        .await;

        result.write_get_metrics(start.elapsed(), KVStoreType::CloudProxy, &self.0.metrics)
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        let start = Instant::now();
        let result = async {
            let url = self.build_url(key);
            let resp = self.0.client.head(url).send().await?;
            let status = resp.status();
            if status == StatusCode::NOT_FOUND {
                return Ok(false);
            }
            if !status.is_success() {
                bail!("cloud proxy exists check failed with status: {status}");
            }
            Ok(true)
        }
        .await;

        result.write_get_metrics(start.elapsed(), KVStoreType::CloudProxy, &self.0.metrics)
    }

    async fn metadata(&self, key: &str) -> Result<Option<ObjectMeta>> {
        use sha2::{Digest, Sha256};

        let start = Instant::now();
        let result = async {
            // Same GET the proxy `get` performs -- the proxy has no cheaper HEAD
            // that returns a checksum, so we issue the GET and use the proxy's
            // checksum when present, hashing the returned body otherwise.
            let url = self.build_url(key);
            let resp = self.0.client.get(url).send().await?;
            let status = resp.status();
            if status == StatusCode::NOT_FOUND {
                return Ok(None);
            }
            if !status.is_success() {
                bail!("cloud proxy metadata get failed with status: {status}");
            }
            let resp: ProxyResponse = resp.json().await?;

            if let Some(checksum_b64) = resp.checksum {
                let decoded = base64::prelude::BASE64_STANDARD.decode(checksum_b64)?;
                let checksum_sha256: [u8; 32] = decoded
                    .as_slice()
                    .try_into()
                    .map_err(|_| eyre!("cloud proxy checksum had unexpected length"))?;
                return Ok(Some(ObjectMeta { checksum_sha256 }));
            }

            // No checksum from the proxy: hash the body we already fetched.
            let bytes = base64::prelude::BASE64_STANDARD.decode(resp.data)?;
            let checksum_sha256: [u8; 32] = Sha256::digest(&bytes).into();
            Ok(Some(ObjectMeta { checksum_sha256 }))
        }
        .await;

        result.write_get_metrics(start.elapsed(), KVStoreType::CloudProxy, &self.0.metrics)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TX_HASH: &str = "0x0000000000000000000000000000000000000000000000000000000000000001";

    #[test]
    fn proxy_response_deserializes_with_checksum() {
        let json = format!(r#"{{"tx_hash":"{TX_HASH}","data":"YWJj","checksum":"YWJjZA=="}}"#);
        let resp: ProxyResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(resp.checksum.as_deref(), Some("YWJjZA=="));
        assert_eq!(resp.data, "YWJj");
    }

    #[test]
    fn proxy_response_deserializes_without_checksum() {
        // Older proxy responses omit `checksum`; `#[serde(default)]` must allow it.
        let json = format!(r#"{{"tx_hash":"{TX_HASH}","data":"YWJj"}}"#);
        let resp: ProxyResponse = serde_json::from_str(&json).unwrap();
        assert!(resp.checksum.is_none());
        assert_eq!(resp.data, "YWJj");
    }

    #[test]
    fn proxy_response_deserializes_with_null_checksum() {
        let json = format!(r#"{{"tx_hash":"{TX_HASH}","data":"YWJj","checksum":null}}"#);
        let resp: ProxyResponse = serde_json::from_str(&json).unwrap();
        assert!(resp.checksum.is_none());
    }
}
