use std::time::Instant;

use alloy_primitives::{Address, U256};
use alloy_rpc_client::ClientBuilder;
use futures::future::join_all;

async fn run_benchmark(rpc_url: &str, num_requests: usize, batch_size: usize) {
    let url = reqwest::Url::parse(rpc_url).unwrap();
    let client = ClientBuilder::default().http(url);

    let addresses: Vec<Address> = (0..num_requests).map(|_| Address::repeat_byte(3)).collect();

    let start = Instant::now();

    let mut total_successful = 0;

    for chunk in addresses.chunks(batch_size) {
        // create a new batch request
        let mut batch = client.new_batch();

        // add each call in the chunk to the batch
        let mut futures = Vec::with_capacity(chunk.len());
        for address in chunk {
            let balance_fut = batch
                .add_call::<(&Address, &str), U256>("eth_getBalance", &(address, "latest"))
                .unwrap();
            futures.push(balance_fut);
        }

        // send the batch and wait for the results
        if batch.send().await.is_ok() {
            let results = join_all(futures).await;

            let successful = results.iter().filter(|r| r.is_ok()).count();
            total_successful += successful;
        } else {
            println!("Batch request failed");
        }
    }

    let duration = start.elapsed();

    println!(
        "Time taken to process {} requests: {:?}",
        num_requests, duration
    );
    println!(
        "Average time per request: {:?}",
        duration / num_requests as u32
    );
    println!("Successful requests: {}/{}", total_successful, num_requests);
}

// Prerequisite is to run an rpc server at http://localhost:8080 before running this binary
#[tokio::main]
async fn main() {
    let rpc_url = "http://localhost:8080";
    let num_requests = 10000; // total number of requests to send
    let batch_size = 2000; // maximum number of requests per batch
    run_benchmark(rpc_url, num_requests, batch_size).await;
}
