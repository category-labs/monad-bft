use std::{
    collections::HashMap,
    ops::{Index, IndexMut},
};

#[derive(Default)]
pub struct ExecutorMetrics(HashMap<&'static str, u64>);

impl Index<&'static str> for ExecutorMetrics {
    type Output = u64;

    fn index(&self, index: &'static str) -> &Self::Output {
        self.0.get(index).unwrap_or(&0)
    }
}

impl IndexMut<&'static str> for ExecutorMetrics {
    fn index_mut(&mut self, index: &'static str) -> &mut Self::Output {
        self.0.entry(index).or_default()
    }
}

impl AsRef<Self> for ExecutorMetrics {
    fn as_ref(&self) -> &Self {
        self
    }
}

impl<'a> From<&'a ExecutorMetrics> for ExecutorMetricsChain<'a> {
    fn from(metrics: &'a ExecutorMetrics) -> Self {
        ExecutorMetricsChain(vec![metrics])
    }
}

#[derive(Default)]
pub struct ExecutorMetricsChain<'a>(Vec<&'a ExecutorMetrics>);
impl<'a> ExecutorMetricsChain<'a> {
    pub fn chain(mut self, metrics: ExecutorMetricsChain<'a>) -> Self {
        self.0.extend(metrics.0);
        self
    }
}
