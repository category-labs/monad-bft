use monad_dataplane::metrics::DataplaneExecutorMetrics;
use monad_metrics::{executor_metrics, MetricsPolicy};

executor_metrics! {
    RaptorCast {
        namespace: raptorcast,
        counters: [
            a
        ],
    }
}

pub struct RaptorCastDataplaneMetrics<MP>
where
    MP: MetricsPolicy,
{
    pub raptorcast: RaptorCastExecutorMetrics<MP>,
    pub dataplane: DataplaneExecutorMetrics<MP>,
}

impl<MP> Default for RaptorCastDataplaneMetrics<MP>
where
    MP: MetricsPolicy,
    MP::Counter: Default,
    MP::Gauge: Default,
{
    fn default() -> Self {
        Self {
            raptorcast: Default::default(),
            dataplane: Default::default(),
        }
    }
}
