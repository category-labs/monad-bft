use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{error, info};

#[derive(Debug, Serialize, Deserialize)]
pub struct RpcRequest {
    #[serde(rename = "nodeName")]
    pub node_name: String,
    #[serde(rename = "blockNumber")]
    pub block_number: u64,
    pub request: Value,
    pub response: Value,
}

#[derive(Debug, Deserialize)]
struct ComparisonResponse {
    status: String,
}

#[derive(Clone)]
pub struct RpcComparator {
    endpoint: String,
    node_name: String,
    client: Client,
}

impl RpcComparator {
    pub fn new(endpoint: String, node_name: String) -> Self {
        Self {
            endpoint,
            node_name,
            client: Client::new(),
        }
    }

    pub async fn submit_comparison(&self, block_number: u64, request: Value, response: Value) {
        let request = RpcRequest {
            node_name: self.node_name.clone(),
            block_number,
            request,
            response,
        };

        info!(
            "Submitting comparison request for block {}, node {}",
            block_number, self.node_name
        );

        match self
            .client
            .post(format!("{}/addRequest", self.endpoint))
            .json(&request)
            .send()
            .await
        {
            Ok(response) => {
                if !response.status().is_success() {
                    error!(
                        "Failed to submit comparison request: HTTP {}",
                        response.status()
                    );
                    return;
                }

                match response.json::<ComparisonResponse>().await {
                    Ok(comparison_response) => {
                        info!("Comparison server response: {}", comparison_response.status);
                    }
                    Err(e) => {
                        error!("Failed to parse comparison response: {}", e);
                    }
                }
            }
            Err(e) => {
                error!("Failed to submit comparison request: {}", e);
            }
        }
    }
}
