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
use eyre::Context;
use reqwest::header::HeaderValue;
use url::Url;

use crate::{
    error::{ErrorKind, Result, ResultExt},
    prelude::*,
};

#[derive(Clone)]
pub struct CloudProxyReader(Arc<CloudProxyReaderInner>);

struct CloudProxyReaderInner {
    pub client: reqwest::Client,
    pub url: Url,
    pub table: String,
}

impl CloudProxyReader {
    pub fn new(api_key: &str, url: Url, table: String) -> Result<Self> {
        let mut headers = reqwest::header::HeaderMap::with_capacity(1);
        headers.insert(
            "x-api-key",
            HeaderValue::from_str(api_key)
                .wrap_err("Invalid API key header value")
                .kind(ErrorKind::Validation)?,
        );
        let client = reqwest::ClientBuilder::new()
            .default_headers(headers)
            .timeout(Duration::from_secs(2))
            .build()
            .wrap_err("Failed to build reqwest client")
            .kind(ErrorKind::Network)?;
        Ok(Self(Arc::new(CloudProxyReaderInner { client, url, table })))
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct ProxyResponse {
    tx_hash: TxHash,
    data: String,
}

impl KVReader for CloudProxyReader {
    async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        let url = format!("{}?txhash={}&table={}", self.0.url, key, self.0.table);
        let resp = self
            .0
            .client
            .get(url)
            .send()
            .await
            .wrap_err("Cloud proxy request failed")
            .kind(ErrorKind::Network)?;
        if resp.status() == 404 {
            return Ok(None);
        }
        let resp: ProxyResponse = resp
            .json()
            .await
            .wrap_err("Cloud proxy response parse failed")
            .kind(ErrorKind::Network)?;
        let bytes = base64::prelude::BASE64_STANDARD
            .decode(resp.data)
            .wrap_err("Cloud proxy base64 decode failed")
            .kind(ErrorKind::Parse)?;

        Ok(Some(bytes.into()))
    }
}
