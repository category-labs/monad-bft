use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::RwLock;
use std::sync::RwLockReadGuard;
use std::sync::RwLockWriteGuard;

use tracing_core::field::Visit;
use tracing_core::{span, Event, Interest, Metadata, Subscriber as CoreSubscriber};
use tracing_subscriber::layer::Filter;

const METRIC_PREFIX_MONOTONIC_COUNTER: &str = "monotonic_counter.";
const METRIC_STATUS: &str = "metric_status";

#[macro_export]
macro_rules! counter_status {
    () => {{
        use tracing::trace;
        trace!(metric_status = true)
    }};
}

#[macro_export]
macro_rules! inc_count {
    ($($k:ident).+) => {{
        use tracing::trace;
        trace!(monotonic_counter.$($k).+ = 1);
    }};
}

pub struct MetricFilter;

impl<S> Filter<S> for MetricFilter {
    fn enabled(
        &self,
        meta: &Metadata<'_>,
        _cx: &tracing_subscriber::layer::Context<'_, S>,
    ) -> bool {
        for field in meta.fields() {
            if field.name().starts_with(METRIC_PREFIX_MONOTONIC_COUNTER)
                || field.name() == METRIC_STATUS
            {
                return false;
            }
        }
        true
    }
}

struct Visitor<'a> {
    counts: &'a RwLock<HashMap<String, AtomicUsize>>,
    prefix: RwLockReadGuard<'a, String>,
}

impl<'a> Visitor<'a> {
    fn increment(&self, key: &String, value: usize) {
        {
            let lock = self.counts.read().unwrap();
            if let Some(metric) = lock.get(key) {
                metric.fetch_add(value, Ordering::Release);
                return;
            }
        }
        let mut lock = self.counts.write().unwrap();
        let metric = lock
            .entry(key.to_string())
            .or_insert_with(|| AtomicUsize::new(0));
        metric.fetch_add(value, Ordering::Release);
    }

    fn status(&self) {
        let lock = self.counts.read().unwrap();
        let mut freq = lock
            .iter()
            .map(|(k, v)| (k, v.load(Ordering::Acquire)))
            .collect::<Vec<_>>();
        freq.sort_by_key(|&(k, _v)| k);
        println!("Counter service: {:?}", freq);
    }
}

impl<'a> Visit for Visitor<'a> {
    fn record_debug(&mut self, _field: &tracing::field::Field, _value: &dyn std::fmt::Debug) {
        // noop
    }

    fn record_i64(&mut self, field: &tracing_core::Field, value: i64) {
        if let Some(metric) = field.name().strip_prefix(METRIC_PREFIX_MONOTONIC_COUNTER) {
            let metric = self.prefix.clone() + metric;
            self.increment(&metric, value as usize)
        }
    }

    fn record_u64(&mut self, field: &tracing_core::Field, value: u64) {
        if let Some(metric) = field.name().strip_prefix(METRIC_PREFIX_MONOTONIC_COUNTER) {
            let metric = self.prefix.clone() + metric;
            self.increment(&metric, value as usize)
        }
    }

    fn record_bool(&mut self, field: &tracing_core::Field, _value: bool) {
        if field.name() == "metric_status" {
            self.status()
        }
    }
}

pub struct CounterLayer {
    counts: RwLock<HashMap<String, AtomicUsize>>,
    spans: RwLock<HashMap<span::Id, String>>,
    prefix: RwLock<String>,
}

impl CounterLayer {
    pub fn new() -> Self {
        Self {
            counts: RwLock::new(HashMap::new()),
            spans: RwLock::new(HashMap::new()),
            prefix: RwLock::new(String::new()),
        }
    }

    fn visitor(&self) -> Visitor<'_> {
        Visitor {
            counts: &self.counts,
            prefix: self.prefix.read().unwrap(),
        }
    }

    fn span_vistor(&self, span_id: &span::Id) -> NewSpanVisitor<'_> {
        NewSpanVisitor {
            spans: self.spans.write().unwrap(),
            span_id: span_id.clone(),
        }
    }
}

impl Default for CounterLayer {
    fn default() -> Self {
        Self::new()
    }
}

struct NewSpanVisitor<'a> {
    spans: RwLockWriteGuard<'a, HashMap<span::Id, String>>,
    span_id: span::Id,
}

impl<'a> Visit for NewSpanVisitor<'a> {
    fn record_debug(&mut self, field: &tracing_core::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "id" {
            self.spans
                .insert(self.span_id.clone(), format!("{:?}:", value));
        }
    }
}

impl<S> tracing_subscriber::Layer<S> for CounterLayer
where
    S: CoreSubscriber,
    Self: 'static,
{
    fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
        let mut interest = Interest::sometimes();
        for key in metadata.fields() {
            if key.name().starts_with(METRIC_PREFIX_MONOTONIC_COUNTER)
                || key.name() == METRIC_STATUS
            {
                interest = Interest::always();
            }
        }

        interest
    }

    fn on_new_span(
        &self,
        attrs: &span::Attributes<'_>,
        id: &span::Id,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if attrs.metadata().name() == "node" {
            let values = attrs.values();
            let mut visitor = self.span_vistor(id);
            values.record(&mut visitor);
        }
    }

    fn on_event(&self, event: &Event<'_>, _ctx: tracing_subscriber::layer::Context<'_, S>) {
        let mut visitor = self.visitor();
        event.record(&mut visitor);
    }

    fn on_enter(&self, id: &span::Id, _ctx: tracing_subscriber::layer::Context<'_, S>) {
        let spans_r = self.spans.read().unwrap();

        if let Some(span_info) = spans_r.get(id) {
            self.prefix.write().unwrap().push_str(span_info);
        }
    }

    fn on_exit(&self, id: &span::Id, _ctx: tracing_subscriber::layer::Context<'_, S>) {
        let spans_r = self.spans.read().unwrap();
        if let Some(span_info) = spans_r.get(id) {
            let mut lock_prefix = self.prefix.write().unwrap();
            let new_prefix = lock_prefix.trim_end_matches(span_info);
            *lock_prefix = new_prefix.to_string();
        }
    }

    fn on_close(&self, id: span::Id, _ctx: tracing_subscriber::layer::Context<'_, S>) {
        // span id management
        self.spans.write().unwrap().remove(&id);
    }

    fn on_id_change(
        &self,
        _old: &span::Id,
        _new: &span::Id,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        // TODO: move prefix to a different id
    }
}
