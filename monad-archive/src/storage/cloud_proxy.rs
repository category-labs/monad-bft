use std::{collections::HashMap, time::Duration};

use alloy_primitives::TxHash;
use alloy_rlp::Decodable;
use base64::{prelude::BASE64_STANDARD, Engine};
use eyre::{Context, Result};
use futures::{StreamExt, TryStreamExt};
use reqwest::header::{HeaderMap, HeaderValue};
use tokio_retry::Retry;
use url::Url;

use crate::{retry_strategy, IndexStoreReader, TxIndexedData};

#[derive(Clone)]
pub struct CloudProxyReader {
    pub client: reqwest::Client,
    pub url: Url,
    pub table: String,
    pub concurrency: usize,
}

impl CloudProxyReader {
    pub fn new(api_key: &str, url: Url, table: String, concurrency: usize) -> Result<Self> {
        let mut headers = reqwest::header::HeaderMap::with_capacity(1);
        headers.insert("x-api-key", HeaderValue::from_str(api_key)?);
        let client = reqwest::ClientBuilder::new()
            .default_headers(headers)
            .timeout(Duration::from_secs(2))
            .build()?;
        Ok(Self {
            client,
            url,
            table,
            concurrency,
        })
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct ProxyResponse {
    tx_hash: TxHash,
    data: String,
}

impl IndexStoreReader for CloudProxyReader {
    async fn bulk_get(&self, keys: &[TxHash]) -> Result<HashMap<TxHash, TxIndexedData>> {
        // TODO: real bulk requests
        futures::stream::iter(keys)
            .map(|key| {
                Retry::spawn(retry_strategy(), move || {
                    let this = self.clone();
                    async move { this.get(key).await.map(|data| (data, key)) }
                })
            })
            .buffered(self.concurrency)
            .try_fold(
                HashMap::with_capacity(keys.len()),
                |mut map, (maybe_data, key)| async move {
                    if let Some(data) = maybe_data {
                        map.insert(*key, data);
                    }
                    Ok(map)
                },
            )
            .await
    }

    async fn get(&self, key: &TxHash) -> Result<Option<TxIndexedData>> {
        let url = format!("{}?txhash={:x}&table={}", self.url, key, self.table);
        let resp = self.client.get(url).send().await?;
        if resp.status() == 404 {
            return Ok(None);
        }
        let resp: ProxyResponse = resp.json().await?;
        let bytes = base64::prelude::BASE64_STANDARD.decode(resp.data)?;

        let data = TxIndexedData::decode(&mut bytes.as_slice())
            .wrap_err("Failed to rlp decode TxIndexedData from cloud proxy")?;
        Ok(Some(data))
    }
}
