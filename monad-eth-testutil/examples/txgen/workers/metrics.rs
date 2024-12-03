use std::sync::RwLock;

use futures::join;

use super::*;

#[derive(Default)]
pub struct Metrics {
    pub accts_with_nonzero_bal: AtomicUsize,
    pub total_txs_sent: AtomicUsize,
    pub total_rpc_calls: AtomicUsize,
    pub total_committed_txs: AtomicUsize,

    pub receipts_rpc_calls: AtomicUsize,
    pub receipts_rpc_calls_error: AtomicUsize,
    pub receipts_tx_success: AtomicUsize,
    pub receipts_tx_failure: AtomicUsize,
    pub receipts_contracts_deployed: AtomicUsize,
    pub receipts_gas_consumed: Arc<RwLock<U256>>,

    pub logs_rpc_calls: AtomicUsize,
    pub logs_rpc_calls_error: AtomicUsize,
    pub logs_total: AtomicUsize,
    // pub logs_erc20_transfers: AtomicUsize,
    // pub logs_erc20_total_value_transfered: Arc<RwLock<U256>>,

    // pub txs_by_hash_rpc_calls: AtomicUsize,
    // pub txs_by_hash_rpc_calls_error: AtomicUsize,
}

impl Metrics {
    pub async fn run(self: Arc<Metrics>) {
        let secs_5 = self.metrics_at_timestep(Duration::from_secs(5));
        let min_1 = self.metrics_at_timestep(Duration::from_secs(60));
        let min_60 = self.metrics_at_timestep(Duration::from_secs(60 * 60));

        join!(secs_5, min_1, min_60);
    }

    async fn metrics_at_timestep(&self, report_interval: Duration) {
        let mut report_interval = tokio::time::interval(report_interval);
        report_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last = Instant::now();

        // Basic metrics
        let mut nonzero_accts = Rate::new(&self.accts_with_nonzero_bal);
        let mut txs_sent = Rate::new(&self.total_txs_sent);
        let mut rpc_calls = Rate::new(&self.total_rpc_calls);
        let mut committed_txs = Rate::new(&self.total_committed_txs);

        // Receipt metrics
        let mut receipts_rpc_calls = Rate::new(&self.receipts_rpc_calls);
        let mut receipts_rpc_calls_error = Rate::new(&self.receipts_rpc_calls_error);
        let mut receipts_tx_success = Rate::new(&self.receipts_tx_success);
        let mut receipts_tx_failure = Rate::new(&self.receipts_tx_failure);
        let mut receipts_contracts_deployed = Rate::new(&self.receipts_contracts_deployed);
        // let mut receipts_gas_consumed = Rate::new(&self.receipts_gas_consumed);

        // Logs metrics
        let mut logs_rpc_calls = Rate::new(&self.logs_rpc_calls);
        let mut logs_rpc_calls_error = Rate::new(&self.logs_rpc_calls_error);
        let mut logs_total = Rate::new(&self.logs_total);

        loop {
            let now = report_interval.tick().await;
            let elapsed = last.elapsed().as_secs_f64();

            // Note: can't call rate twice, so must make var if used twice
            let tx_success_ps = receipts_tx_success.rate(elapsed);

            info!(
                nonzero_accts = nonzero_accts.val(),
                sent_txs = txs_sent.val(),
                committed_txs = committed_txs.val(),
                rps = rpc_calls.rate(elapsed),
                accts_created_ps = nonzero_accts.rate(elapsed),
                tx_success_ps,
                committed_tps = committed_txs.rate(elapsed),
                tps = txs_sent.rate(elapsed),
                "Metrics          (freq: {}m:{}s)",
                report_interval.period().as_secs() / 60,
                report_interval.period().as_secs() % 60
            );

            info!(
                contracts_deployed = receipts_contracts_deployed.val(),
                tx_success = receipts_tx_success.val(),
                tx_failure = receipts_tx_failure.val(),
                rpc_calls = receipts_rpc_calls.val(),
                rpc_calls_error = receipts_rpc_calls_error.val(),
                contracts_deployed_ps = receipts_contracts_deployed.rate(elapsed),
                rpc_calls_error_ps = receipts_rpc_calls_error.rate(elapsed),
                rpc_calls_ps = receipts_rpc_calls.rate(elapsed),
                tx_failure_ps = receipts_tx_failure.rate(elapsed),
                tx_success_ps,
                "Metrics Receipts (freq: {}m:{}s)",
                report_interval.period().as_secs() / 60,
                report_interval.period().as_secs() % 60
            );

            info!(
                rpc_calls = logs_rpc_calls.val(),
                rpc_calls_error = logs_rpc_calls_error.val(),
                total = logs_total.val(),
                rpc_calls_ps = logs_rpc_calls.rate(elapsed),
                rpc_calls_error_ps = logs_rpc_calls_error.rate(elapsed),
                total_ps = logs_total.rate(elapsed),
                "Metrics Logs     (freq: {}m:{}s)",
                report_interval.period().as_secs() / 60,
                report_interval.period().as_secs() % 60
            );
            last = now;
        }
    }
}

struct Rate<'a> {
    val: &'a AtomicUsize,
    prev: usize,
}

impl<'a> Rate<'a> {
    fn new(val: &'a AtomicUsize) -> Rate<'a> {
        Rate {
            val,
            prev: val.load(SeqCst),
        }
    }

    fn rate(&mut self, elapsed: f64) -> usize {
        let curr = self.val();
        let diff = curr - self.prev;
        let raw = (diff) as f64 / elapsed;
        self.prev = curr;
        raw.round() as usize
    }

    fn val(&self) -> usize {
        self.val.load(SeqCst)
    }
}
