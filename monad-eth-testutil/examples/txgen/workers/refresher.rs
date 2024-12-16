use tokio::time::MissedTickBehavior;

use crate::shared::erc20::ERC20;

use super::*;

pub struct Refresher {
    pub rpc_rx: mpsc::UnboundedReceiver<AccountsWithTime>,
    pub gen_sender: mpsc::Sender<Accounts>,

    pub client: ReqwestClient,
    pub metrics: Arc<Metrics>,

    pub delay: Duration,

    pub deployed_erc20: ERC20,
}

impl Refresher {
    pub async fn run(mut self) {
        let mut interval = tokio::time::interval(Duration::from_millis(50));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        info!("Starting refresher loop");
        while let Some(AccountsWithTime { accts, sent }) = self.rpc_rx.recv().await {
            info!(
                num_accts = accts.len(),
                channel_len = self.rpc_rx.len(),
                "Refresher received accts"
            );
            if sent + self.delay >= Instant::now() {
                tokio::time::sleep_until(sent + self.delay).await;
                debug!("Refresher waited delay, refreshing batch...");
            }

            interval.tick().await;

            self.handle_batch(accts);
        }
    }

    fn handle_batch(&self, mut accts: Accounts) {
        let client = self.client.clone();
        let metrics = self.metrics.clone();
        let gen_sender = self.gen_sender.clone();
        let deployed_erc20 = self.deployed_erc20;

        tokio::spawn(async move {
            let mut times_sent = 0;

            while let Err(e) = refresh_batch(&client, &mut accts, &metrics, deployed_erc20).await {
                if times_sent > 5 {
                    error!("Exhausted retries refreshing account, oh well! {e}");
                } else {
                    times_sent += 1;
                    warn!(
                        times_sent,
                        "Encountered error refreshing accts, retrying..., {e}"
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }

            debug!("Completed batch refresh, sending to gen...");
            gen_sender.send(accts).await.expect("gen rx closed");
            debug!("Refresher sent batch to gen");
        });
    }
}

pub async fn refresh_batch(
    client: &ReqwestClient,
    accts: &mut Accounts,
    metrics: &Metrics,
    deployed_erc20: ERC20,
) -> Result<()> {
    trace!("Refreshing batch...");

    let iter = accts.iter().map(|a| &a.addr);
    let (native_bals, nonces, erc20_bals) = tokio::join!(
        client.batch_get_balance(iter.clone()),
        client.batch_get_transaction_count(iter.clone()),
        client.batch_get_erc20_balance(iter.clone(), deployed_erc20),
    );

    let native_bals = native_bals?;
    let nonces = nonces?;
    let erc20_bals = erc20_bals?;

    metrics
        .total_rpc_calls
        .fetch_add(accts.iter().len() * 2, SeqCst);

    for (i, acct) in accts.iter_mut().enumerate() {
        if let Ok((_, b)) = &native_bals[i] {
            acct.native_bal = *b;
        }
        if let Ok((_, n)) = &nonces[i] {
            acct.nonce = *n;
        }
        if let Ok((_, b)) = &erc20_bals[i] {
            acct.erc20_bal = *b;
        }
    }
    trace!("Batch refreshed");

    Ok(())
}
