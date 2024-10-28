use futures::join;

use super::*;

#[derive(Default)]
pub struct Metrics {
    pub accts_with_nonzero_bal: AtomicUsize,
    pub total_txs_sent: AtomicUsize,
    pub total_rpc_calls: AtomicUsize,
    pub total_committed_txs: AtomicUsize,
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

        let mut prev_non_zero = self.accts_with_nonzero_bal.load(SeqCst);
        let mut prev_txs_sent = self.total_txs_sent.load(SeqCst);
        let mut prev_rpc_calls = self.total_rpc_calls.load(SeqCst);
        let mut prev_committed_txs = self.total_committed_txs.load(SeqCst);
        let mut last = Instant::now();

        loop {
            let now = report_interval.tick().await;
            let elapsed = last.elapsed().as_secs_f64();

            let (total_nonzero_accts, accts_created_per_sec) =
                rate(&mut prev_non_zero, &self.accts_with_nonzero_bal, elapsed);
            let (total_sent_txs, tps) = rate(&mut prev_txs_sent, &self.total_txs_sent, elapsed);
            let (_, rps) = rate(&mut prev_rpc_calls, &self.total_rpc_calls, elapsed);
            let (total_committed_txs, committed_tps) =
                rate(&mut prev_committed_txs, &self.total_committed_txs, elapsed);

            let seconds = report_interval.period().as_secs() % 60;
            let minutes = report_interval.period().as_secs() / 60;

            info!(
                total_nonzero_accts,
                total_sent_txs,
                total_committed_txs,
                rps,
                accts_created_per_sec,
                committed_tps,
                tps,
                "Metrics (Interval: {}:{})",
                minutes,
                seconds
            );

            last = now;
        }
    }
}

fn rate(prev: &mut usize, a_curr: &AtomicUsize, elapsed: f64) -> (usize, usize) {
    let curr = a_curr.load(SeqCst);
    let diff = curr - *prev;
    let raw = (diff) as f64 / elapsed;
    *prev = curr;
    (curr, raw.round() as usize)
}
