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

use std::{
    io::Write,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use eyre::bail;
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use serde::{Deserialize, Serialize};

use crate::{
    config::{Config, DeployedContract, TrafficGen},
    generators::make_generator,
    prelude::*,
    shared::{ecmul::ECMul, erc20::ERC20, eth_json_rpc::EthJsonRpc, uniswap::Uniswap},
};

pub async fn run(client: ReqwestClient, config: Config) -> Result<()> {
    if config.traffic_gen.is_empty() {
        bail!("No traffic generation configurations provided");
    }

    let mut traffic_index = 0;

    loop {
        let current_traffic_gen = &config.traffic_gen[traffic_index];
        info!(
            "Starting traffic generation phase {}: {:?}",
            traffic_index, current_traffic_gen.gen_mode
        );

        run_traffic_phase(&client, &config, current_traffic_gen).await?;

        traffic_index = (traffic_index + 1) % config.traffic_gen.len();
    }
}

async fn run_traffic_phase(
    client: &ReqwestClient,
    config: &Config,
    traffic_gen: &TrafficGen,
) -> Result<()> {
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = Arc::clone(&shutdown);

    let (rpc_sender, gen_rx) = mpsc::channel(2);
    let (gen_sender, refresh_rx) = mpsc::channel(100);
    let (refresh_sender, rpc_rx) = mpsc::unbounded_channel();
    let (recipient_sender, recipient_gen_rx) = mpsc::unbounded_channel();

    // simpler to always deploy erc20 even if not used
    let deployed_contract = load_or_deploy_contracts(config, client).await?;

    // kick start cycle by injecting accounts
    generate_sender_groups(config, traffic_gen).for_each(|group| {
        if let Err(e) = refresh_sender.send(group) {
            if shutdown.load(Ordering::Relaxed) {
                debug!("Failed to send account group during shutdown: {}", e);
            } else {
                error!("Failed to send account group unexpectedly: {}", e);
            }
        }
    });

    // shared state for monitoring
    let metrics = Arc::new(Metrics::default());
    let sent_txs = Arc::new(DashMap::with_capacity(traffic_gen.tps as usize * 10));

    // setup metrics and monitoring
    let committed_tx_watcher = CommittedTxWatcher::new(
        client,
        &sent_txs,
        &metrics,
        Duration::from_secs_f64(config.refresh_delay_secs * 2.),
        config,
    )
    .await;

    let recipient_tracker = RecipientTracker {
        rpc_sender_rx: recipient_gen_rx,
        client: client.clone(),
        delay: Duration::from_secs_f64(config.refresh_delay_secs),
        non_zero: Default::default(),
        metrics: Arc::clone(&metrics),
    };

    // primary workers
    let generator = make_generator(config, traffic_gen, deployed_contract.clone())?;
    let gen = GeneratorHarness::new(
        generator,
        refresh_rx,
        rpc_sender,
        client,
        U256::from(config.min_native_amount),
        U256::from(config.seed_native_amount),
        &metrics,
        config.base_fee(),
        config.chain_id,
        traffic_gen.gen_mode.clone(),
        Arc::clone(&shutdown),
    );

    let refresher = Refresher::new(
        rpc_rx,
        gen_sender,
        client.clone(),
        Arc::clone(&metrics),
        Duration::from_secs_f64(config.refresh_delay_secs),
        deployed_contract,
        traffic_gen.erc20_balance_of,
        traffic_gen.gen_mode.clone(),
        Arc::clone(&shutdown),
    )?;

    let rpc_sender = RpcSender::new(
        gen_rx,
        refresh_sender,
        recipient_sender,
        client.clone(),
        Arc::clone(&metrics),
        sent_txs,
        config,
        traffic_gen,
        Arc::clone(&shutdown),
    );

    let metrics_reporter = MetricsReporter::new(
        metrics.clone(),
        config.otel_endpoint.clone(),
        config.otel_replica_name.clone(),
        format!("{:?}", traffic_gen.gen_mode),
    )?;

    let mut tasks = FuturesUnordered::new();

    // abort if critical task stops
    tasks.push(critical_task("Rpc Sender", tokio::spawn(rpc_sender.run())).boxed());
    tasks.push(critical_task("Generator Harness", tokio::spawn(gen.run())).boxed());
    tasks.push(critical_task("Refresher", tokio::spawn(refresher.run())).boxed());

    // continue working if helper task stops
    tasks.push(
        helper_task(
            "Metrics",
            tokio::spawn(metrics.run(Arc::clone(&shutdown))),
            Arc::clone(&shutdown),
        )
        .boxed(),
    );
    tasks.push(
        helper_task(
            "Otel Reporter",
            tokio::spawn(metrics_reporter.run(Arc::clone(&shutdown))),
            Arc::clone(&shutdown),
        )
        .boxed(),
    );
    tasks.push(
        helper_task(
            "Recipient Tracker",
            tokio::spawn(recipient_tracker.run(Arc::clone(&shutdown))),
            Arc::clone(&shutdown),
        )
        .boxed(),
    );
    tasks.push(
        helper_task(
            "CommittedTx Watcher",
            tokio::spawn(committed_tx_watcher.run()),
            Arc::clone(&shutdown),
        )
        .boxed(),
    );

    let timeout = tokio::time::sleep(Duration::from_secs(traffic_gen.runtime));

    tokio::select! {
        _ = timeout => {
            info!("Traffic phase completed after {} seconds", traffic_gen.runtime);
            shutdown_clone.store(true, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(())
        }
        result = tasks.next() => {
            match result {
                Some(Ok(_)) => {
                    info!("Task completed successfully");
                    Ok(())
                }
                Some(Err(e)) => {
                    info!("Task failed: {e:?}");
                    Err(e)
                }
                None => Ok(()),
            }
        }
    }
}

async fn helper_task(
    name: &'static str,
    task: tokio::task::JoinHandle<()>,
    shutdown: Arc<AtomicBool>,
) -> Result<()> {
    let res = task.await;
    match res {
        Ok(_) => info!("Helper task {name} shut down"),
        Err(e) => {
            if shutdown.load(Ordering::Relaxed) {
                info!("Helper task {name} terminated during shutdown. Error: {e}");
            } else {
                error!("Helper task {name} terminated unexpectedly. Error: {e}");
            }
        }
    }
    Ok(())
}

async fn critical_task(name: &'static str, task: tokio::task::JoinHandle<()>) -> Result<()> {
    let res = task.await;
    use eyre::WrapErr;
    match res {
        Ok(_) => Err(eyre::eyre!("Critical task {name} shut down")),
        Err(e) => Err(e).context("Critical task {name} terminated"),
    }
}

fn generate_sender_groups<'a>(
    config: &'a Config,
    traffic_gen: &'a TrafficGen,
) -> impl Iterator<Item = AccountsWithTime> + 'a {
    let mut rng = SmallRng::seed_from_u64(traffic_gen.sender_seed);
    let num_groups = (config.senders(traffic_gen) / config.sender_group_size(traffic_gen)).max(1);
    let mut key_iter = config.root_private_keys.iter();

    (0..num_groups).map(move |_| AccountsWithTime {
        accts: Accounts {
            accts: (0..config.sender_group_size(traffic_gen))
                .map(|_| PrivateKey::new_with_random(&mut rng))
                .map(SimpleAccount::from)
                .collect(),
            root: key_iter
                .next()
                .map(PrivateKey::new)
                .map(SimpleAccount::from),
        },
        sent: Instant::now() - Duration::from_secs_f64(config.refresh_delay_secs),
    })
}

async fn verify_contract_code(client: &ReqwestClient, addr: Address) -> Result<bool> {
    let code = client.get_code(&addr).await?;
    Ok(code != "0x")
}

#[derive(Deserialize, Serialize)]
struct DeployedContractFile {
    erc20: Option<Address>,
    ecmul: Option<Address>,
    uniswap: Option<Address>,
}

async fn load_or_deploy_contracts(
    config: &Config,
    client: &ReqwestClient,
) -> Result<DeployedContract> {
    use crate::config::RequiredContract;

    // Use first contract for now, might want to check all in the future
    let contract_to_ensure = if !config.traffic_gen.is_empty() {
        config.required_contract(&config.traffic_gen[0])
    } else {
        RequiredContract::None
    };
    let path = "deployed_contracts.json";
    let deployer = PrivateKey::new(&config.root_private_keys[0]);
    let max_fee_per_gas = config.base_fee() * 2;
    let chain_id = config.chain_id;

    match contract_to_ensure {
        RequiredContract::None => Ok(DeployedContract::None),
        RequiredContract::ERC20 => {
            match open_deployed_contracts_file(path) {
                Ok(DeployedContractFile {
                    erc20: Some(erc20), ..
                }) => {
                    if verify_contract_code(client, erc20).await? {
                        info!("Contract loaded from file validated");
                        return Ok(DeployedContract::ERC20(ERC20 { addr: erc20 }));
                    }
                    warn!(
                        "Contract loaded from file not found on chain, deploying new contract..."
                    );
                }
                Err(e) => info!("Failed to load deployed contracts file, {e}"),
                _ => info!("Contract not in deployed contracts file"),
            }

            // if not found, deploy new contract
            let erc20 = ERC20::deploy(&deployer, client, max_fee_per_gas, chain_id).await?;

            let deployed = DeployedContractFile {
                erc20: Some(erc20.addr),
                ecmul: None,
                uniswap: None,
            };

            write_and_verify_deployed_contracts(client, path, &deployed).await?;
            Ok(DeployedContract::ERC20(erc20))
        }
        RequiredContract::ECMUL => {
            match open_deployed_contracts_file(path) {
                Ok(DeployedContractFile {
                    ecmul: Some(ecmul), ..
                }) => {
                    if verify_contract_code(client, ecmul).await? {
                        info!("Contract loaded from file validated");
                        return Ok(DeployedContract::ECMUL(ECMul { addr: ecmul }));
                    }
                    warn!(
                        "Contract loaded from file not found on chain, deploying new contract..."
                    );
                }
                Err(e) => info!("Failed to load deployed contracts file, {e}"),
                _ => info!("Contract not in deployed contracts file"),
            }

            // if not found, deploy new contract
            let ecmul = ECMul::deploy(&deployer, client, max_fee_per_gas, chain_id).await?;

            let deployed = DeployedContractFile {
                erc20: None,
                ecmul: Some(ecmul.addr),
                uniswap: None,
            };

            write_and_verify_deployed_contracts(client, path, &deployed).await?;
            Ok(DeployedContract::ECMUL(ecmul))
        }
        RequiredContract::Uniswap => {
            match open_deployed_contracts_file(path) {
                Ok(DeployedContractFile {
                    uniswap: Some(uniswap),
                    ..
                }) => {
                    if verify_contract_code(client, uniswap).await? {
                        info!("Contract loaded from file validated");
                        return Ok(DeployedContract::Uniswap(Uniswap { addr: uniswap }));
                    }
                    warn!(
                        "Contract loaded from file not found on chain, deploying new contract..."
                    );
                }
                Err(e) => info!("Failed to load deployed contracts file, {e}"),
                _ => info!("Contract not in deployed contracts file"),
            }

            // if not found, deploy new contract
            let uniswap = Uniswap::deploy(&deployer, client, max_fee_per_gas, chain_id).await?;

            let deployed = DeployedContractFile {
                erc20: None,
                ecmul: None,
                uniswap: Some(uniswap.addr),
            };

            write_and_verify_deployed_contracts(client, path, &deployed).await?;
            Ok(DeployedContract::Uniswap(uniswap))
        }
    }
}

fn open_deployed_contracts_file(path: &str) -> Result<DeployedContractFile> {
    std::fs::File::open(path)
        .context("Failed to open deployed contracts file")
        .and_then(|file| {
            serde_json::from_reader::<_, DeployedContractFile>(file)
                .context("Failed to parse deployed contracts")
        })
}

async fn write_and_verify_deployed_contracts(
    client: &ReqwestClient,
    path: &str,
    dc: &DeployedContractFile,
) -> Result<()> {
    if let Some(addr) = dc.erc20 {
        if !verify_contract_code(client, addr).await? {
            bail!("Failed to verify freshly deployed contract");
        }
    }
    if let Some(addr) = dc.ecmul {
        if !verify_contract_code(client, addr).await? {
            bail!("Failed to verify freshly deployed contract");
        }
    }

    let mut file = std::fs::File::create(path)?;
    serde_json::to_writer(&mut file, &dc).context("Failed to serialize deployed contracts")?;
    file.flush()?;
    info!("Wrote deployed contract addresses to {path}");

    Ok(())
}
