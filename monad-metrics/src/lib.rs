use instruments::{NoopCounter, NoopGauge};
pub use monad_metrics_macro::{executor_metrics, state_metrics};

pub use self::instruments::{Counter, Gauge, StaticCounter, StaticGauge};
use self::instruments::{LocalCounter, LocalGauge};

mod instruments;

pub trait MetricsPolicy: std::fmt::Debug + Unpin + Send + Sync + 'static {
    type Counter: Counter;
    type Gauge: Gauge;
}

#[derive(Debug)]
pub struct StaticMetricsPolicy;

impl MetricsPolicy for StaticMetricsPolicy {
    type Counter = StaticCounter;
    type Gauge = StaticGauge;
}

#[derive(Debug)]
pub struct MockMetricsPolicy;

impl MetricsPolicy for MockMetricsPolicy {
    type Counter = LocalCounter;
    type Gauge = LocalGauge;
}

#[derive(Debug)]
pub struct NoopMetricsPolicy;

impl MetricsPolicy for NoopMetricsPolicy {
    type Counter = NoopCounter;
    type Gauge = NoopGauge;
}
