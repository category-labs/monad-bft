use std::{io::Write, str::FromStr};

use eyre::bail;
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use serde::{Deserialize, Serialize};

use crate::{
    generators::make_generator,
    prelude::*,
    shared::{ecmul::ECMul, erc20::ERC20, eth_json_rpc::EthJsonRpc, uniswap::Uniswap},
    DeployedContract,
};

pub async fn run(client: ReqwestClient, config: Config) -> Result<()> {
    let (rpc_sender, gen_rx) = mpsc::channel(2);
    let (gen_sender, refresh_rx) = mpsc::channel(100);
    let (refresh_sender, rpc_rx) = mpsc::unbounded_channel();
    let (recipient_sender, recipient_gen_rx) = mpsc::unbounded_channel();

    // simpler to always deploy erc20 even if not used
    let deployed_contract = load_or_deploy_contracts(&config, &client).await?;

    // kick start cycle by injecting accounts
    generate_sender_groups(&config).for_each(|group| refresh_sender.send(group).unwrap());

    // shared state for monitoring
    let metrics = Arc::new(Metrics::default());
    let sent_txs = Arc::new(DashMap::with_capacity(config.tps as usize * 10));

    // setup metrics and monitoring
    let committed_tx_watcher = CommittedTxWatcher::new(
        &client,
        &sent_txs,
        &metrics,
        Duration::from_secs_f64(config.refresh_delay_secs * 2.),
        &config,
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
    let generator = make_generator(&config, deployed_contract.clone())?;
    let gen = GeneratorHarness::new(
        generator,
        refresh_rx,
        rpc_sender,
        &client,
        U256::from(config.min_native_amount),
        U256::from(config.seed_native_amount),
        &metrics,
        config.base_fee(),
        config.chain_id,
    );

    let refresher = Refresher {
        rpc_rx,
        gen_sender,
        client: client.clone(),
        delay: Duration::from_secs_f64(config.refresh_delay_secs),
        metrics: Arc::clone(&metrics),
        deployed_contract,
        refresh_erc20_balance: config.erc20_balance_of,
    };

    let rpc_sender = RpcSender {
        gen_rx,
        recipient_sender,
        refresh_sender,
        client: client.clone(),
        target_tps: config.tps,
        metrics: Arc::clone(&metrics),
        sent_txs,
        verbose: config.verbose,
    };

    let mut tasks = FuturesUnordered::new();

    // abort if critical task stops
    tasks.push(critical_task("Rpc Sender", tokio::spawn(rpc_sender.run())).boxed());
    tasks.push(critical_task("Generator Harness", tokio::spawn(gen.run())).boxed());
    tasks.push(critical_task("Refresher", tokio::spawn(refresher.run())).boxed());

    // continue working if helper task stops
    tasks.push(helper_task("Metrics", tokio::spawn(metrics.run())).boxed());
    tasks.push(helper_task("Recipient Tracker", tokio::spawn(recipient_tracker.run())).boxed());
    tasks.push(
        helper_task(
            "Committed Tx Watcher",
            tokio::spawn(committed_tx_watcher.run()),
        )
        .boxed(),
    );

    while let Some(res) = tasks.next().await {
        if let Err(e) = res {
            error!("Task terminated with error: {e}");
            return Err(e);
        }
    }
    Ok(())
}

async fn helper_task(name: &'static str, task: tokio::task::JoinHandle<()>) -> Result<()> {
    let res = task.await;
    match res {
        Ok(_) => info!("Helper task {name} shut down"),
        Err(e) => error!("Helper task {name} terminated, continuing. Error: {e}"),
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

fn generate_sender_groups(config: &Config) -> impl Iterator<Item = AccountsWithTime> + '_ {
    let mut rng = SmallRng::seed_from_u64(config.sender_seed);
    let num_groups = config.senders() / config.sender_group_size();
    let mut key_iter = config.root_private_keys.iter();

    (0..num_groups).map(move |_| AccountsWithTime {
        accts: Accounts {
            accts: (0..config.sender_group_size())
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
    let contract_to_ensure = config.required_contract();
    let path = "deployed_contracts.json";
    let deployer = PrivateKey::new(&config.root_private_keys[0]);
    let max_fee_per_gas = config.base_fee() * 2;
    let chain_id = config.chain_id;

    match contract_to_ensure {
        crate::RequiredContract::None => Ok(DeployedContract::None),
        crate::RequiredContract::ERC20 => {
            // try from commmand line arg
            if let Some(erc20) = &config.erc20_contract {
                let erc20 = Address::from_str(erc20)
                    .wrap_err("Failed to parse erc20 contract string arg")?;
                if verify_contract_code(client, erc20).await? {
                    info!("Contract from cmdline args validated");
                    return Ok(DeployedContract::ERC20(ERC20 { addr: erc20 }));
                }
            };

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
        crate::RequiredContract::ECMUL => {
            // try from commmand line arg
            if let Some(ecmul) = &config.ecmul_contract {
                let ecmul = Address::from_str(ecmul)
                    .wrap_err("Failed to parse ecmul contract string arg")?;
                if verify_contract_code(client, ecmul).await? {
                    info!("Contract from cmdline args validated");
                    return Ok(DeployedContract::ECMUL(ECMul { addr: ecmul }));
                }
            };

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
        crate::RequiredContract::Uniswap => {
            // try from commmand line arg
            if let Some(uniswap) = &config.uniswap_contract {
                let uniswap = Address::from_str(uniswap)
                    .wrap_err("Failed to parse uniswap contract string arg")?;
                if verify_contract_code(client, uniswap).await? {
                    info!("Contract from cmdline args validated");
                    return Ok(DeployedContract::Uniswap(Uniswap { addr: uniswap }));
                }
            };

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
