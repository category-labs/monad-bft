use std::{collections::HashMap, time::Duration};

use alloy_primitives::TxHash;
use base64::Engine;
use eyre::{Context, Result};
use futures::{StreamExt, TryStreamExt};
use reqwest::header::HeaderValue;
use tokio_retry::Retry;
use url::Url;

use super::retry_strategy;
use crate::prelude::*;

#[derive(Clone)]
pub struct CloudProxyReader<BDR = BlockDataReaderErased> {
    pub block_data_reader: BDR,
    pub client: reqwest::Client,
    pub url: Url,
    pub table: String,
    pub concurrency: usize,
}

impl<BDR: BlockDataReader> CloudProxyReader<BDR> {
    pub fn new(
        block_data_reader: BDR,
        api_key: &str,
        url: Url,
        table: String,
        concurrency: usize,
    ) -> Result<Self> {
        let mut headers = reqwest::header::HeaderMap::with_capacity(1);
        headers.insert("x-api-key", HeaderValue::from_str(api_key)?);
        let client = reqwest::ClientBuilder::new()
            .default_headers(headers)
            .timeout(Duration::from_secs(2))
            .build()?;
        Ok(Self {
            block_data_reader,
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

impl<BDR: BlockDataReader> IndexStoreReader for CloudProxyReader<BDR> {
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

        let decoded = IndexDataStorageRepr::decode(&bytes)
            .wrap_err("Failed to decode index data repr in CloudProxyReader")?;
        let latest = decoded
            .convert(&self.block_data_reader)
            .await
            .wrap_err("Failed to convert to latest index data repr in CloudProxyReader")?;
        Ok(Some(latest))
    }
}
