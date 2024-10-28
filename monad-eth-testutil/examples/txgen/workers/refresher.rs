use tokio::time::MissedTickBehavior;

use super::*;

pub struct Refresher {
    pub rpc_rx: mpsc::UnboundedReceiver<AccountsWithTime>,
    pub gen_sender: mpsc::Sender<Accounts>,

    pub client: ReqwestClient,
    pub erc20: ERC20,
    pub erc20_check_bals: bool,
    pub metrics: Arc<Metrics>,

    pub delay: Duration,
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
        let erc20 = self.erc20;
        let metrics = self.metrics.clone();
        let gen_sender = self.gen_sender.clone();
        let erc20_check_bals = self.erc20_check_bals;

        tokio::spawn(async move {
            let mut times_sent = 0;

            while let Err(e) =
                refresh_batch(&client, &erc20, &mut accts, &metrics, erc20_check_bals).await
            {
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
    _erc20: &ERC20,
    accts: &mut Accounts,
    metrics: &Metrics,
    erc20_check_bals: bool, // todo: less jank
) -> Result<()> {
    if erc20_check_bals {
        return refresh_batch_w_erc20(client, _erc20, accts, metrics).await;
    }

    trace!("Refreshing batch...");

    let iter = accts.iter().map(|a| &a.addr);
    let (native_bals, nonces) = tokio::join!(
        client.batch_get_balance(iter.clone()),
        client.batch_get_transaction_count(iter),
    );

    let native_bals = native_bals?;
    let nonces = nonces?;

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
    }
    trace!("Batch refreshed");

    Ok(())
}

/// problematic version to show erc20 behaving badly
pub async fn refresh_batch_w_erc20(
    client: &ReqwestClient,
    erc20: &ERC20,
    accts: &mut Accounts,
    metrics: &Metrics,
) -> Result<()> {
    trace!("Refreshing batch...");

    let iter = accts.iter().map(|a| &a.addr);
    let (erc20_res, native_bals, nonces) = tokio::join!(
        client.batch_get_erc20_balance(iter.clone(), *erc20),
        client.batch_get_balance(iter.clone()),
        client.batch_get_transaction_count(iter),
    );

    let native_bals = native_bals?;
    let nonces = nonces?;
    let erc20_bals = erc20_res?;

    metrics.total_rpc_calls.fetch_add(accts.len() * 3, SeqCst);

    for (i, acct) in accts.iter_mut().enumerate() {
        if let Ok((_, b)) = &erc20_bals[i] {
            acct.erc20_bal = *b;
        }
        if let Ok((_, b)) = &native_bals[i] {
            acct.native_bal = *b;
        }
        if let Ok((_, n)) = &nonces[i] {
            acct.nonce = *n;
        }
    }
    trace!("Batch refreshed");

    Ok(())
}
