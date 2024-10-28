use super::*;
use crate::shared::blockstream::BlockStream;

pub struct CommittedTxWatcher {
    sent_txs: Arc<DashMap<TxHash, Instant>>,
    metrics: Arc<Metrics>,
    delay: Duration,
    blockstream: BlockStream,
}

impl CommittedTxWatcher {
    pub async fn new(
        client: &ReqwestClient,
        sent_txs: &Arc<DashMap<TxHash, Instant>>,
        metrics: &Arc<Metrics>,
        delay: Duration,
    ) -> Self {
        Self {
            sent_txs: Arc::clone(sent_txs),
            metrics: Arc::clone(metrics),
            delay,
            blockstream: BlockStream::new(client.clone(), Duration::from_millis(50), false)
                .await
                .expect("Failed to fetch initial block number for blockstream"),
        }
    }

    pub async fn run(mut self) {
        while let Some(block) = self.blockstream.next().await {
            let block = match block {
                Ok(b) => b,
                Err(e) => {
                    warn!("Blockstream returned error: {e}");
                    continue;
                }
            };

            let mut ours = 0;
            for hash in block.transactions.hashes() {
                if self.sent_txs.remove(hash).is_some() {
                    ours += 1;
                }
            }

            self.metrics.total_committed_txs.fetch_add(ours, SeqCst);

            let now = Instant::now();
            self.sent_txs.retain(|_, v| *v + self.delay > now);
        }
    }
}
