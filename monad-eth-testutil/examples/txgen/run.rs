use std::{io::Write, str::FromStr};

use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use serde::{Deserialize, Serialize};

use crate::{
    generators::make_generator,
    prelude::*,
    shared::{erc20::ERC20, eth_json_rpc::EthJsonRpc},
};

pub async fn run(client: ReqwestClient, config: Config) -> Result<()> {
    let (rpc_sender, gen_rx) = mpsc::channel(10);
    let (gen_sender, refresh_rx) = mpsc::channel(100);
    let (refresh_sender, rpc_rx) = mpsc::unbounded_channel();
    let (recipient_sender, recipient_gen_rx) = mpsc::unbounded_channel();

    // simpler to always deploy erc20 even if not used
    let erc20 = load_or_deploy_erc20(&config, &client).await?;

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
    )
    .await;

    let recipient_tracker = RecipientTracker {
        rpc_sender_rx: recipient_gen_rx,
        client: client.clone(),
        erc20,
        delay: Duration::from_secs_f64(config.refresh_delay_secs),
        non_zero: Default::default(),
        metrics: Arc::clone(&metrics),
        erc20_balance_of: config.erc20_balance_of,
    };

    // primary workers
    let generator = make_generator(&config, erc20);
    let gen = GeneratorHarness::new(
        generator,
        refresh_rx,
        rpc_sender,
        &client,
        erc20,
        U256::from(1e15),
        U256::from(1e18),
        &metrics,
    );

    let refresher = Refresher {
        rpc_rx,
        gen_sender,
        client: client.clone(),
        erc20,
        delay: Duration::from_secs_f64(config.refresh_delay_secs),
        metrics: Arc::clone(&metrics),
        erc20_check_bals: config.erc20_balance_of,
    };

    let rpc_sender = RpcSender {
        gen_rx,
        recipient_sender,
        refresh_sender,
        client: client.clone(),
        target_tps: config.tps,
        metrics: Arc::clone(&metrics),
        sent_txs,
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

async fn load_or_deploy_erc20(config: &Config, client: &ReqwestClient) -> Result<ERC20> {
    if let Some(addr) = &config.erc20_contract {
        return Ok(ERC20 {
            addr: Address::from_str(addr).context("failed to parse erc20 contract string")?,
        });
    }

    let deployer = PrivateKey::new(&config.root_private_keys[0]);

    // try reading deployed contracts from file
    let path = "deployed_contracts.json";
    let res = (|| -> Result<ERC20> {
        let file = std::fs::File::open(path)?;
        let deployed_contracts: DeployedContract = serde_json::from_reader(&file)?;
        info!(
            "Loaded erc20 from {path}, address: {}",
            deployed_contracts.erc20
        );
        let erc20 = ERC20 {
            addr: deployed_contracts.erc20,
        };

        Ok(erc20)
    })();
    if let Ok(erc20) = res {
        if client.get_erc20_balance(&deployer.0, erc20).await.is_ok() {
            warn!(
                address = erc20.addr.to_string(),
                "erc20 address read from file does not exist on-chain, deploying a new contract..."
            );
            return res;
        }
    }

    let erc20 = ERC20::deploy(&deployer, client).await?;

    // write deployed_contracts to file
    let mut file = std::fs::File::create(path)?;
    serde_json::to_writer(&mut file, &DeployedContract { erc20: erc20.addr })
        .context("Failed to serialize deployed contracts")?;
    info!("Wrote deployed contract files to disk");
    file.flush()?;

    Ok(erc20)
}

#[derive(Deserialize, Serialize)]
struct DeployedContract {
    erc20: Address,
}
