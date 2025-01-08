use tokio::time::MissedTickBehavior;

use super::*;

pub struct RpcSender {
    pub gen_rx: mpsc::Receiver<AccountsWithTxs>,
    pub refresh_sender: mpsc::UnboundedSender<AccountsWithTime>,
    pub recipient_sender: mpsc::UnboundedSender<AddrsWithTime>,

    pub client: ReqwestClient,
    pub target_tps: u64,
    pub metrics: Arc<Metrics>,
    pub sent_txs: Arc<DashMap<TxHash, Instant>>,

    pub verbose: bool,
}

impl RpcSender {
    pub async fn run(mut self) {
        let mut interval = tokio::time::interval(Duration::from_millis(
            BATCH_SIZE as u64 * 1000 / self.target_tps,
        ));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        info!(
            interval_ms = interval.period().as_millis(),
            "Starting rpc sender loop"
        );
        while let Some(AccountsWithTxs { accts, txs }) = self.gen_rx.recv().await {
            info!(
                num_accts = accts.len(),
                num_txs = txs.len(),
                channel_len = self.gen_rx.len(),
                "RpcSender received accounts with txs"
            );

            for batch in txs.chunks(BATCH_SIZE) {
                self.spawn_send_batch(batch);

                // limit sending batch by interval
                interval.tick().await;
            }

            debug!("Sending accts to refresher...");
            self.refresh_sender
                .send(AccountsWithTime {
                    accts,
                    sent: Instant::now(),
                })
                .expect("Sender not closed");
            debug!("Accts sent to refresher...");
        }
    }

    fn spawn_send_batch(&self, batch: &[(TxEnvelope, Address)]) {
        if batch.is_empty() {
            return; // unnecessary?
        }
        trace!(batch_size = batch.len(), "Sending batch of txs...");

        let recipient_sender = self.recipient_sender.clone();
        let client = self.client.clone();
        let metrics = self.metrics.clone();
        let sent_txs = self.sent_txs.clone();
        let batch = Vec::from_iter(batch.iter().cloned()); // todo: make more performant
        let verbose = self.verbose;

        tokio::spawn(async move {
            let now = Instant::now();
            for (tx, to) in &batch {
                let _ = sent_txs.insert(*tx.tx_hash(), now);
                if verbose {
                    trace!(
                        tx_hash = tx.tx_hash().to_string(),
                        to = to.to_string(),
                        "Tx"
                    );
                }
            }

            send_batch(&client, batch.iter().map(|(tx, _)| tx), &metrics).await;

            trace!("Tx batch sent, sending accts to recipient tracker...");
            recipient_sender
                .send(AddrsWithTime {
                    addrs: batch.iter().map(|(_, a)| *a).collect(),
                    sent: Instant::now(),
                })
                .expect("recipient tracker rx closed");

            trace!("Sent accts to recipient tracker");
        });
    }
}

pub async fn send_batch(
    client: &ReqwestClient,
    txs: impl Iterator<Item = &TxEnvelope>,
    metrics: &Metrics,
) {
    let now = Instant::now();

    let mut batch_req = client.new_batch();

    let mut futs = txs
        .filter_map(|tx| {
            batch_req
                .add_call::<_, TxHash>("eth_sendRawTransaction", &[alloy_rlp::encode(tx)])
                .ok() // todo: handle better
        })
        .collect::<FuturesUnordered<_>>();

    if let Err(e) = batch_req.send().await {
        error!("Failed to send batch: {e}");
        return;
    }

    let num_txs = futs.len();
    metrics.total_txs_sent.fetch_add(num_txs, SeqCst);

    while let Some(resp) = futs.next().await {
        if let Err(e) = resp {
            error!("Failed to send tx: {e}");
        }
    }

    trace!(elapsed_ms = now.elapsed().as_millis(), "send_batch latency");
}
