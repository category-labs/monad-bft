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
    future::Future,
    io::Write,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

use chrono::Utc;
use eyre::bail;
use futures::{future::try_join_all, stream::FuturesUnordered, FutureExt};
use serde::{Deserialize, Serialize};

use crate::{
    config::{Config, DeployedContract, TrafficGen},
    generators::make_generator,
    prelude::{
        rpc_request_gen::{RpcWalletSpam, RpcWsCompare},
        *,
    },
    report::Report,
    shared::{
        ecmul::ECMul, eip7702::EIP7702, erc20::ERC20, eth_json_rpc::EthJsonRpc, nft_sale::NftSale,
        uniswap::Uniswap,
    },
};

/// Runs the txgen for the given config
///
/// This function will run each workload group in sequence, and when the runtime of the current workload group is reached, it will move to the next workload group.
/// It will repeat this process until all workload groups have been run.
///
/// Each workload group can contain one or more traffic gens. The txgen will run each traffic gen in parallel
pub async fn run(clients: Vec<ReqwestClient>, config: Config) -> Result<()> {
    if config.workload_groups.is_empty() {
        bail!("No workload group configurations provided");
    }

    let mut workload_group_index = 0;
    let read_client = clients[0].clone();

    loop {
        let workload_group = &config.workload_groups[workload_group_index];
        info!(
            "Starting workload group phase {}: {}",
            workload_group_index, workload_group.name
        );

        let start_time = Utc::now();
        let metrics = Arc::new(Metrics::default());
        if let Err(e) =
            run_workload_group(&clients, &config, workload_group_index, metrics.clone()).await
        {
            error!(
                workload_group = workload_group.name,
                "Failed to run workload group: {e:?}"
            );
        }

        if let Some(report_dir) = config.report_dir.as_deref() {
            let report = Report::new(
                config.clone(),
                workload_group_index,
                start_time,
                &metrics,
                &read_client,
                config.prom_url.clone(),
            )
            .await;

            if let Err(e) = report.to_json_file(report_dir.as_ref()) {
                error!("Failed to write report: {e:?}");
            }
        };

        workload_group_index = (workload_group_index + 1) % config.workload_groups.len();
    }
}

/// Runs the workload group for the given config
///
/// This function will run each traffic gen in the workload group in parallel
async fn run_workload_group(
    clients: &[ReqwestClient],
    config: &Config,
    workload_group_index: usize,
    metrics: Arc<Metrics>,
) -> Result<()> {
    let workload_group = &config.workload_groups[workload_group_index];
    let read_client = clients[0].clone();

    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = Arc::clone(&shutdown);

    // shared state for monitoring
    let sent_txs = Arc::new(DashMap::with_capacity(100_000));

    // Shared tasks for all workers in the workload group
    let mut tasks = FuturesUnordered::new();

    if workload_group.spam_rpc_ws {
        let spammer = RpcWalletSpam::new(
            read_client.clone(),
            config.ws_url().expect("WS URL is not valid"),
            workload_group.num_ws_connections,
        );
        let shutdown_clone = Arc::clone(&shutdown);
        tasks.push(
            critical_task(
                "Spammer",
                tokio::spawn(async move { spammer.run(shutdown_clone).await }),
            )
            .boxed(),
        );
    }

    if workload_group.compare_rpc_ws {
        let compare_rpc_ws = RpcWsCompare::new(
            read_client.clone(),
            config.ws_url().expect("WS URL is not valid"),
        );
        let shutdown_clone = Arc::clone(&shutdown);
        tasks.push(
            critical_task(
                "Compare RPC WS",
                tokio::spawn(async move { compare_rpc_ws.run(shutdown_clone).await }),
            )
            .boxed(),
        );
    }

    // Deployed contract for each traffic gen
    let mut deployed_contracts = Vec::new();
    for traffic_gen in &workload_group.traffic_gens {
        // deploy contracts for each traffic gen in the workload group
        let deployed_contract = load_or_deploy_contracts(config, traffic_gen, &read_client).await?;
        deployed_contracts.push(deployed_contract.clone());

        tasks.extend(run_traffic_gen(
            clients,
            config,
            workload_group,
            traffic_gen,
            &shutdown,
            metrics.clone(),
            deployed_contract,
            sent_txs.clone(),
        )?);
    }

    // setup metrics and monitoring
    let committed_tx_watcher = CommittedTxWatcher::new(
        &read_client,
        &sent_txs,
        &metrics,
        Duration::from_secs_f64(config.refresh_delay_secs * 2.),
        config,
    )
    .await;

    let metrics_reporter = MetricsReporter::new(
        metrics.clone(),
        config.otel_endpoint.clone(),
        config.otel_replica_name.clone(),
        workload_group.name.clone(),
    )?;

    // continue working if helper task stops
    tasks.push(
        helper_task(
            "Metrics",
            tokio::spawn(metrics.clone().run(Arc::clone(&shutdown))),
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
            "CommittedTx Watcher",
            tokio::spawn(committed_tx_watcher.run()),
            Arc::clone(&shutdown),
        )
        .boxed(),
    );

    let runtime_seconds = (workload_group.runtime_minutes * 60.) as u64;
    let timeout = tokio::time::sleep(Duration::from_secs(runtime_seconds));

    // Wait for all tasks to complete or timeout
    tokio::select! {
        _ = timeout => {
            info!("Traffic phase completed after {} minutes", workload_group.runtime_minutes);
            shutdown_clone.store(true, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(())
        }
        result = try_join_all(tasks) => {
            match result {
                Ok(_) => {
                    info!("Task completed successfully");
                    Ok(())
                }
                Err(e) => {
                    info!("Task failed: {e:?}");
                    Err(e)
                }
            }
        }
    }
}

fn run_traffic_gen(
    clients: &[ReqwestClient],
    config: &Config,
    workload_group: &WorkloadGroup,
    traffic_gen: &TrafficGen,
    shutdown: &Arc<AtomicBool>,
    metrics: Arc<Metrics>,
    deployed_contract: DeployedContract,
    sent_txs: Arc<DashMap<TxHash, Instant>>,
) -> Result<impl Iterator<Item = Pin<Box<dyn Future<Output = Result<()>> + Send>>>> {
    let read_client = clients[0].clone();

    let (rpc_sender, gen_rx) = mpsc::channel(2);
    let (gen_sender, refresh_rx) = async_channel::bounded::<Accounts>(100);
    let (refresh_sender, rpc_rx) = mpsc::unbounded_channel();
    let base_fee = Arc::new(Mutex::new(
        // safe to default to 0; it'll get set later by the refresher
        // TODO share base_fee across all traffic gens?
        0_u128,
    ));

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

    let generator = make_generator(traffic_gen, deployed_contract.clone())?;
    let gen = GeneratorHarness::new(
        generator,
        refresh_rx,
        rpc_sender,
        &read_client,
        U256::from_str_radix(&config.min_native_amount, 10).unwrap(),
        U256::from_str_radix(&config.seed_native_amount, 10).unwrap(),
        &metrics,
        &base_fee,
        config.chain_id,
        traffic_gen.gen_mode.clone(),
        config.gas_limit_contract_deployment,
        config.set_tx_gas_limit,
        config.priority_fee,
        config.random_priority_fee_range,
        workload_group.mutation_percentage,
        Arc::clone(shutdown),
    );

    let rpc_sender = RpcSender::new(
        gen_rx,
        refresh_sender,
        clients.to_vec(),
        Arc::clone(&metrics),
        sent_txs,
        config,
        traffic_gen,
        Arc::clone(shutdown),
    );

    let refresher = Refresher::new(
        rpc_rx,
        gen_sender,
        read_client,
        Arc::clone(&metrics),
        base_fee,
        Duration::from_secs_f64(config.refresh_delay_secs),
        deployed_contract,
        traffic_gen.erc20_balance_of,
        workload_group.name.clone(),
        Arc::clone(shutdown),
    )?;

    Ok([
        critical_task("Refresher", tokio::spawn(refresher.run())).boxed(),
        critical_task("Rpc Sender", tokio::spawn(rpc_sender.run())).boxed(),
        critical_task("Generator Harness", tokio::spawn(gen.run())).boxed(),
    ]
    .into_iter())
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
    let num_groups = (traffic_gen.senders() / traffic_gen.sender_group_size()).max(1);
    let mut key_iter = config.root_private_keys.iter();

    (0..num_groups).map(move |_| AccountsWithTime {
        accts: Accounts {
            accts: (0..traffic_gen.sender_group_size())
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
    eip7702: Option<Address>,
    nft_sale: Option<Address>,
}

async fn load_or_deploy_contracts(
    config: &Config,
    traffic_gen: &TrafficGen,
    client: &ReqwestClient,
) -> Result<DeployedContract> {
    use crate::config::RequiredContract;

    let contract_to_ensure = traffic_gen.required_contract();

    const PATH: &str = "deployed_contracts.json";
    let deployer = PrivateKey::new(&config.root_private_keys[0]);
    let base_fee = client.get_base_fee().await?;
    let max_fee_per_gas = base_fee * 2;
    let chain_id = config.chain_id;

    match contract_to_ensure {
        RequiredContract::None => Ok(DeployedContract::None),
        RequiredContract::ERC20 => {
            // Check CLI override first
            if let Some(addr_str) = &config.erc20_contract {
                if let Ok(addr) = addr_str.parse::<Address>() {
                    if verify_contract_code(client, addr).await? {
                        info!("Using ERC20 contract from CLI: {}", addr);
                        return Ok(DeployedContract::ERC20(ERC20 { addr }));
                    } else {
                        warn!("ERC20 contract from CLI has no code, falling back to auto-deploy");
                    }
                }
            }

            match open_deployed_contracts_file(PATH) {
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
            let erc20 = ERC20::deploy(
                &deployer,
                client,
                max_fee_per_gas,
                chain_id,
                config.gas_limit_contract_deployment,
            )
            .await?;

            let deployed = DeployedContractFile {
                erc20: Some(erc20.addr),
                ecmul: None,
                uniswap: None,
                eip7702: None,
                nft_sale: None,
            };

            write_and_verify_deployed_contracts(client, PATH, &deployed).await?;
            Ok(DeployedContract::ERC20(erc20))
        }
        RequiredContract::ECMUL => {
            match open_deployed_contracts_file(PATH) {
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
            let ecmul = ECMul::deploy(
                &deployer,
                client,
                max_fee_per_gas,
                chain_id,
                config.gas_limit_contract_deployment,
            )
            .await?;

            let deployed = DeployedContractFile {
                erc20: None,
                ecmul: Some(ecmul.addr),
                uniswap: None,
                eip7702: None,
                nft_sale: None,
            };

            write_and_verify_deployed_contracts(client, PATH, &deployed).await?;
            Ok(DeployedContract::ECMUL(ecmul))
        }
        RequiredContract::Uniswap => {
            match open_deployed_contracts_file(PATH) {
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
            let uniswap = Uniswap::deploy(
                &deployer,
                client,
                max_fee_per_gas,
                chain_id,
                config.gas_limit_contract_deployment,
            )
            .await?;

            let deployed = DeployedContractFile {
                erc20: None,
                ecmul: None,
                uniswap: Some(uniswap.addr),
                eip7702: None,
                nft_sale: None,
            };

            write_and_verify_deployed_contracts(client, PATH, &deployed).await?;
            Ok(DeployedContract::Uniswap(uniswap))
        }
        RequiredContract::EIP7702 => {
            match open_deployed_contracts_file(PATH) {
                Ok(DeployedContractFile {
                    eip7702: Some(eip7702),
                    ..
                }) => {
                    if verify_contract_code(client, eip7702).await? {
                        info!("Contract loaded from file validated");
                        return Ok(DeployedContract::EIP7702(EIP7702 { addr: eip7702 }));
                    }
                    warn!(
                        "Contract loaded from file not found on chain, deploying new contract..."
                    );
                }
                Err(e) => info!("Failed to load deployed contracts file, {e}"),
                _ => info!("Contract not in deployed contracts file"),
            }

            // if not found, deploy new contract
            let eip7702 = EIP7702::deploy(
                &deployer,
                client,
                max_fee_per_gas,
                chain_id,
                config.gas_limit_contract_deployment,
            )
            .await?;

            let deployed = DeployedContractFile {
                erc20: None,
                ecmul: None,
                uniswap: None,
                eip7702: Some(eip7702.addr),
                nft_sale: None,
            };

            write_and_verify_deployed_contracts(client, PATH, &deployed).await?;
            Ok(DeployedContract::EIP7702(eip7702))
        }
        RequiredContract::NftSale => {
            match open_deployed_contracts_file(PATH) {
                Ok(DeployedContractFile {
                    nft_sale: Some(nft_sale),
                    ..
                }) => {
                    if verify_contract_code(client, nft_sale).await? {
                        let current_price = NftSale::get_current_price(client, nft_sale).await?;

                        info!("Contract loaded from file validated");
                        return Ok(DeployedContract::NftSale(NftSale {
                            addr: nft_sale,
                            current_sale_price: current_price,
                        }));
                    }
                    warn!(
                        "Contract loaded from file not found on chain, deploying new contract..."
                    );
                }
                Err(e) => info!("Failed to load deployed contracts file, {e}"),
                _ => info!("Contract not in deployed contracts file"),
            }

            // if not found, deploy new contract
            let nft_sale = NftSale::deploy(
                &deployer,
                client,
                max_fee_per_gas,
                chain_id,
                config.gas_limit_contract_deployment,
            )
            .await?;

            let deployed = DeployedContractFile {
                erc20: None,
                ecmul: None,
                uniswap: None,
                eip7702: None,
                nft_sale: Some(nft_sale.addr),
            };

            write_and_verify_deployed_contracts(client, PATH, &deployed).await?;
            Ok(DeployedContract::NftSale(nft_sale))
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
    if let Some(addr) = dc.uniswap {
        if !verify_contract_code(client, addr).await? {
            bail!("Failed to verify freshly deployed contract");
        }
    }
    if let Some(addr) = dc.eip7702 {
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
