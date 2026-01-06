// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::{any::TypeId, cell::RefCell};

use opentelemetry::{metrics::Histogram, KeyValue};
use tracing::{error, field::Visit, span::Id, Dispatch, Span, Subscriber};
use tracing_subscriber::{layer::Context, registry::LookupSpan, Layer};

/// Storage for the chainstate source values in span extensions
/// Tracks all data sources accessed during a span
#[derive(Debug, Clone, Default)]
struct ChainstateSource {
    sources: std::collections::HashSet<String>,
}

/// Visitor to extract a string field value from a span
struct FieldValueVisitor<'a> {
    field_name: &'a str,
    value: Option<String>,
}

impl<'a> FieldValueVisitor<'a> {
    fn new(field_name: &'a str) -> Self {
        Self {
            field_name,
            value: None,
        }
    }
}

impl<'a> Visit for FieldValueVisitor<'a> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == self.field_name {
            self.value = Some(format!("{:?}", value));
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == self.field_name {
            self.value = Some(value.to_string());
        }
    }
}

thread_local! {
    // NOTE(dshulyak) it doesn't matter what we use here for id, it will be always overwritten
    static MAIN_SPAN: RefCell<Id> = RefCell::new(Id::from_u64(u64::MAX));
}

#[derive(Debug)]
enum SpanType {
    Main,
    Secondary(Id),
}

#[derive(Debug)]
struct TimingContext {
    span_type: SpanType,
    // recorded when span entered, cleared on close
    // note that it is important not to clear it on _exit_, as then we will capture await times
    // rather then whole duration
    entered: Option<quanta::Instant>,
    // duration since exit to enter is a total amount of wait time
    // we aggregate that across multiple awaits and capture in single recording
    exited: Option<quanta::Instant>,
    wait_time: f64,
    histogram: Histogram<f64>,
}

pub struct TimingsLayer<S> {
    clock: quanta::Clock,
    main_timings_callback: MainTimingsCallback,
    secondary_timings_callback: MeteredSpanCallback,
    _phantom: std::marker::PhantomData<S>,
}

impl<S> Default for TimingsLayer<S>
where
    S: Subscriber + for<'span> LookupSpan<'span>,
{
    fn default() -> Self {
        Self::new()
    }
}

struct MainTimingsCallback(fn(&Dispatch, &Id, Histogram<f64>));

struct MeteredSpanCallback(fn(&Dispatch, &Id));

impl<S> TimingsLayer<S>
where
    S: Subscriber + for<'span> LookupSpan<'span>,
{
    pub fn new() -> Self {
        Self {
            clock: quanta::Clock::new(),
            main_timings_callback: MainTimingsCallback(Self::update_main),
            secondary_timings_callback: MeteredSpanCallback(Self::update_secondary),
            _phantom: std::marker::PhantomData,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_clock(clock: quanta::Clock) -> Self {
        Self {
            clock,
            ..Self::new()
        }
    }

    fn update_main(d: &Dispatch, id: &Id, histogram: Histogram<f64>) {
        if let Some(ext) = d.downcast_ref::<S>() {
            let span = ext.span(id).expect("span must exist");
            span.extensions_mut().insert(TimingContext {
                span_type: SpanType::Main,
                entered: None,
                histogram,
                wait_time: 0.0,
                exited: None,
            });
        };
    }

    fn update_secondary(d: &Dispatch, id: &Id) {
        let subscriber = match d.downcast_ref::<S>() {
            Some(subscriber) => subscriber,
            None => return,
        };
        let main_span = MAIN_SPAN.with(|span| {
            let id = span.borrow();
            subscriber.span(&id)
        });
        if let Some(main_span) = main_span {
            let span = subscriber.span(id).expect("span must exist");
            span.extensions_mut().insert(TimingContext {
                span_type: SpanType::Secondary(main_span.id()),
                entered: None,
                histogram: main_span
                    .extensions()
                    .get::<TimingContext>()
                    .unwrap()
                    .histogram
                    .clone(),
                wait_time: 0.0,
                exited: None,
            });
        } else {
            error!("main span not found");
        }
    }
}

impl<S> Layer<S> for TimingsLayer<S>
where
    S: Subscriber + for<'span> LookupSpan<'span>,
{
    #[allow(unsafe_code, trivial_casts)]
    unsafe fn downcast_raw(&self, id: TypeId) -> Option<*const ()> {
        match id {
            id if id == TypeId::of::<Self>() => Some(self as *const _ as *const ()),
            id if id == TypeId::of::<MainTimingsCallback>() => {
                Some(&self.main_timings_callback as *const _ as *const ())
            }
            id if id == TypeId::of::<MeteredSpanCallback>() => {
                Some(&self.secondary_timings_callback as *const _ as *const ())
            }
            _ => None,
        }
    }

    fn on_enter(&self, id: &Id, ctx: Context<'_, S>) {
        // update entered timing
        let span = ctx.span(id).expect("span must exist");
        let mut extensions = span.extensions_mut();
        if let Some(timing_context) = extensions.get_mut::<TimingContext>() {
            // async spans will be entered multiple times, we are measuring total time, not just time spent in await
            if timing_context.entered.is_none() {
                timing_context.entered = Some(self.clock.now());
            }
            if let Some(exited) = timing_context.exited.take() {
                timing_context.wait_time += (self.clock.now() - exited).as_secs_f64();
            }
            timing_context.exited = None;
            // if it is main push it to the thread local storage on every enter
            if matches!(timing_context.span_type, SpanType::Main) {
                MAIN_SPAN.replace(id.clone());
            }
        }
    }

    fn on_exit(&self, id: &Id, ctx: Context<'_, S>) {
        let span = ctx.span(id).expect("span must exist");
        let mut extensions = span.extensions_mut();
        if let Some(timing_context) = extensions.get_mut::<TimingContext>() {
            timing_context.exited = Some(self.clock.now());
        }
    }

    fn on_record(
        &self,
        span: &Id,
        values: &tracing::span::Record<'_>,
        ctx: Context<'_, S>,
    ) {
        let span = ctx.span(span).expect("span must exist");
        let mut visitor = FieldValueVisitor::new("chainstate.source");
        values.record(&mut visitor);
        
        if let Some(value) = visitor.value {
            let mut extensions = span.extensions_mut();
            let chainstate_source = extensions
                .get_mut::<ChainstateSource>()
                .map(|s| {
                    s.sources.insert(value.clone());
                });
            
            // If not present, insert a new one
            if chainstate_source.is_none() {
                let mut source = ChainstateSource::default();
                source.sources.insert(value);
                extensions.insert(source);
            }
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let span = ctx.span(&id).expect("span must exist");

        // Extract all chainstate sources used during this span
        // Format as a sorted, comma-separated string for consistent metric labels
        let data_source = span.extensions().get::<ChainstateSource>().map(|s| {
            let mut sources: Vec<_> = s.sources.iter().cloned().collect();
            sources.sort();
            sources.join(",")
        });

        let mut extensions = span.extensions_mut();
        if let Some(timing) = extensions.get_mut::<TimingContext>() {
            if let Some(entered) = timing.entered.take() {
                let elapsed = (self.clock.now() - entered).as_secs_f64();
                match &timing.span_type {
                    SpanType::Secondary(main) => {
                        if let Some(main_span) = ctx.span(main) {
                            let mut attributes = vec![
                                KeyValue::new("main", main_span.name()),
                                KeyValue::new("secondary", span.name()),
                                KeyValue::new("type", "total"),
                            ];
                            if let Some(ref source) = data_source {
                                attributes.push(KeyValue::new("chainstate_source", source.clone()));
                            }
                            timing.histogram.record(elapsed, &attributes);
                            
                            let mut wait_attributes = vec![
                                KeyValue::new("main", main_span.name()),
                                KeyValue::new("secondary", span.name()),
                                KeyValue::new("type", "wait"),
                            ];
                            if let Some(ref source) = data_source {
                                wait_attributes.push(KeyValue::new("chainstate_source", source.clone()));
                            }
                            timing.histogram.record(timing.wait_time, &wait_attributes);
                        } else {
                            error!("main span not found");
                        }
                    }
                    SpanType::Main => {
                        let mut attributes = vec![
                            KeyValue::new("main", span.name()),
                            KeyValue::new("type", "total"),
                        ];
                        if let Some(ref source) = data_source {
                            attributes.push(KeyValue::new("chainstate_source", source.clone()));
                        }
                        timing.histogram.record(elapsed, &attributes);
                        
                        let mut wait_attributes = vec![
                            KeyValue::new("main", span.name()),
                            KeyValue::new("type", "wait"),
                        ];
                        if let Some(ref source) = data_source {
                            wait_attributes.push(KeyValue::new("chainstate_source", source.clone()));
                        }
                        timing.histogram.record(timing.wait_time, &wait_attributes);
                    }
                }
            }
        }
    }
}

pub trait TimingSpanExtension {
    fn with_main_timings(self, histogram: Histogram<f64>) -> Self;
    fn with_sub_timings(self) -> Self;
}

impl TimingSpanExtension for Span {
    fn with_main_timings(self, histogram: Histogram<f64>) -> Self {
        self.with_subscriber(|(id, subscriber)| {
            if let Some(callback) = subscriber.downcast_ref::<MainTimingsCallback>() {
                callback.0(subscriber, id, histogram)
            }
        });
        self
    }

    fn with_sub_timings(self) -> Self {
        self.with_subscriber(|(id, subscriber)| {
            if let Some(callback) = subscriber.downcast_ref::<MeteredSpanCallback>() {
                callback.0(subscriber, id)
            }
        });
        self
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use futures::{executor, future::join_all, FutureExt};
    use opentelemetry::{metrics::MeterProvider, Key, KeyValue};
    use opentelemetry_sdk::metrics::{
        data::{Histogram as HistogramData, HistogramDataPoint},
        InMemoryMetricExporter, PeriodicReader, SdkMeterProvider,
    };
    use tokio::sync::{mpsc, Barrier};
    use tracing::{info_span, Instrument};
    use tracing_subscriber::{layer::SubscriberExt, Registry};

    use crate::{TimingSpanExtension, TimingsLayer};

    pub struct TestContext {
        exporter: InMemoryMetricExporter,
        provider: SdkMeterProvider,
        clock: quanta::Clock,
        mock: Arc<quanta::Mock>,
    }

    impl TestContext {
        pub fn new() -> Self {
            let exporter = InMemoryMetricExporter::default();
            let reader = PeriodicReader::builder(exporter.clone()).build();
            let provider = SdkMeterProvider::builder().with_reader(reader).build();
            let (clock, mock) = quanta::Clock::mock();
            TestContext {
                exporter,
                provider,
                clock,
                mock,
            }
        }

        // run_test_function with thread local subscriber.
        // subscriber will be used for spans created within this function.
        fn run_test_function<F, R>(&self, f: F) -> R
        where
            F: FnOnce() -> R,
        {
            let timings = TimingsLayer::with_clock(self.clock.clone());
            let subscriber = Registry::default().with(timings);

            tracing::subscriber::with_default(subscriber, f)
        }

        // extract_data_points from the first histogram in the exporter.
        fn extract_data_points(&self) -> Vec<HistogramDataPoint<f64>> {
            self.provider.force_flush().expect("flushed metrics");
            let finished = self.exporter.get_finished_metrics().unwrap();
            let histogram = finished.first().expect("histogram must be there");
            let metric = histogram
                .scope_metrics
                .first()
                .expect("exactly 1 scope")
                .metrics
                .first()
                .expect("exactly 1 metric");
            let data = metric
                .data
                .as_any()
                .downcast_ref::<HistogramData<f64>>()
                .unwrap();

            data.data_points.clone()
        }
    }

    #[test]
    fn test_single_sync_span() {
        let buckets = vec![0.0, 10.0, 20.0];
        let samples = [(2, 4), (3, 12), (1, 22)];

        let tctx = TestContext::new();
        let example = tctx
            .provider
            .meter("test")
            .f64_histogram("example")
            .with_boundaries(buckets)
            .build();

        tctx.run_test_function(|| {
            for (count, value) in samples.iter().cloned() {
                (0..count).for_each(|_| {
                    let span = info_span!("first").with_main_timings(example.clone());
                    let _ = span.enter();
                    tctx.mock.increment(Duration::from_secs(value as u64));
                });
            }
        });

        let mut data_points = tctx.extract_data_points();
        data_points.sort_by_key(|k| k.sum as u64);
        assert_eq!(data_points.len(), 2);
        assert_eq!(
            data_points[0].attributes,
            vec![
                KeyValue::new("main", "first"),
                KeyValue::new("type", "wait")
            ]
        );
        assert_eq!(data_points[0].sum, 0.0);

        assert_eq!(
            data_points[1].attributes,
            vec![
                KeyValue::new("main", "first"),
                KeyValue::new("type", "total")
            ]
        );
        let data_point = &data_points[1];
        assert_eq!(
            data_point.count,
            samples.iter().map(|(count, _)| *count).sum::<u64>()
        );
        assert_eq!(
            data_point.sum,
            samples
                .iter()
                .map(|(count, value)| *count as f64 * *value as f64)
                .sum::<f64>()
        );
        // skip 1st bucket as this is one below min value
        assert_eq!(
            data_point.bucket_counts[1..].to_vec(),
            samples.iter().map(|(count, _)| *count).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_multiple_sync_spans() {
        let buckets = (0..10).step_by(1).map(|v| v as f64).collect::<Vec<_>>();
        // (count, time till second, second span duration)
        let samples = [(5, 2, 1), (7, 3, 4), (4, 2, 7)];
        let expected_buckets_first = [0, 0, 0, 5, 0, 0, 0, 7, 0, 4, 0];
        let expected_buckets_second = [0, 5, 0, 0, 7, 0, 0, 4, 0, 0, 0];

        let tctx = TestContext::new();
        let example = tctx
            .provider
            .meter("test")
            .f64_histogram("example")
            .with_boundaries(buckets)
            .build();

        tctx.run_test_function(|| {
            for (count, first, second) in samples {
                for _ in 0..count {
                    let span = info_span!("first").with_main_timings(example.clone());
                    let _enter = span.enter();
                    tctx.mock.increment(Duration::from_secs(first));
                    let span = info_span!("second").with_sub_timings();
                    let _enter = span.enter();
                    tctx.mock.increment(Duration::from_secs(second));
                }
            }
        });

        let mut data_points = tctx.extract_data_points();
        assert_eq!(data_points.len(), 4);
        // NOTE(dshulyak) order is only for assertions
        data_points.sort_by_key(|k| k.sum as u64);
        assert_eq!(
            data_points[3].attributes,
            vec![
                KeyValue::new("main", "first"),
                KeyValue::new("type", "total")
            ]
        );
        assert_eq!(
            data_points[2].attributes,
            vec![
                KeyValue::new("main", "first"),
                KeyValue::new("secondary", "second"),
                KeyValue::new("type", "total")
            ]
        );

        assert_eq!(data_points[3].bucket_counts, expected_buckets_first);
        assert_eq!(data_points[2].bucket_counts, expected_buckets_second);
    }

    #[test]
    fn test_async_multiple_spans() {
        let buckets = (0..10).step_by(1).map(|v| v as f64).collect::<Vec<_>>();
        // (count, time till second, second span duration)
        let samples = [(5, 2, 1), (7, 3, 4), (4, 2, 7)];
        let expected_buckets_first = [0, 0, 0, 5, 0, 0, 0, 7, 0, 4, 0];
        let expected_buckets_second = [0, 5, 0, 0, 7, 0, 0, 4, 0, 0, 0];

        let tctx = TestContext::new();
        let example = tctx
            .provider
            .meter("test")
            .f64_histogram("example")
            .with_boundaries(buckets)
            .build();

        tctx.run_test_function(|| {
            for (count, first, second) in samples {
                for _ in 0..count {
                    let span1 = info_span!("first").with_main_timings(example.clone());
                    let fut1 = async {
                        let span2 = info_span!("second").with_sub_timings();
                        let fut2 = async {
                            tctx.mock.increment(Duration::from_secs(second));
                        };
                        fut2.instrument(span2).await;
                        tctx.mock.increment(Duration::from_secs(first));
                    }
                    .instrument(span1);
                    executor::block_on(fut1);
                }
            }
        });

        let mut data_points = tctx.extract_data_points();
        assert_eq!(data_points.len(), 4);
        // NOTE(dshulyak) order is only for assertions
        data_points.sort_by_key(|k| k.sum as u64);

        assert_eq!(data_points[3].bucket_counts, expected_buckets_first);
        assert_eq!(data_points[2].bucket_counts, expected_buckets_second);
    }

    #[test]
    fn test_async_overlapping_futs() {
        let buckets = (0..10).step_by(1).map(|v| v as f64).collect::<Vec<_>>();
        let tctx = TestContext::new();
        let example = tctx
            .provider
            .meter("test")
            .f64_histogram("example")
            .with_boundaries(buckets)
            .build();

        tctx.run_test_function(|| {
            let spans = [
                (info_span!("1st"), info_span!("1st secondary")),
                (info_span!("2nd"), info_span!("2nd secondary")),
                (info_span!("3rd"), info_span!("3rd secondary")),
            ];
            let barrier = Barrier::new(spans.len());
            let futs = spans.into_iter().map(|(main, secondary)| {
                let main = main.with_main_timings(example.clone());
                async {
                    // with barrier we ensure that the earlier spans will yield and let other to enter
                    // and on enter we overwrite main, and this is what we are testing here
                    barrier.wait().await;
                    let secondary = secondary.with_sub_timings();
                    let fut2 = async {
                        tctx.mock.increment(Duration::from_secs(1));
                    };
                    fut2.instrument(secondary).await;
                }
                .instrument(main)
            });
            executor::block_on(join_all(futs));
        });

        let mut data_points = tctx.extract_data_points();
        assert_eq!(data_points.len(), 12);
        data_points.sort_by_key(|k| k.sum as u64);
        // test that attributes in secondary labels have main label as aa prefix
        for data_point in data_points {
            // there are always atleast 2 attributes
            if data_point.attributes.len() == 2 {
                assert_eq!(data_point.attributes[0].key, Key::from_static_str("main"));
            } else {
                assert_eq!(data_point.attributes[0].key, Key::from_static_str("main"));
                assert_eq!(
                    data_point.attributes[1].key,
                    Key::from_static_str("secondary")
                );
                assert!(data_point.attributes[1].value.as_str().starts_with(
                    data_point.attributes[0]
                        .value
                        .as_str()
                        .into_owned()
                        .as_str()
                ));
            }
        }
    }

    #[test]
    fn test_record_wait_time() {
        let buckets = vec![0.0, 10.0, 20.0];
        let samples = [(2, 4), (3, 12), (1, 22)];

        let tctx = TestContext::new();
        let example = tctx
            .provider
            .meter("test")
            .f64_histogram("example")
            .with_boundaries(buckets)
            .build();

        tctx.run_test_function(|| {
            let (ping_sender, mut ping_receiver) = mpsc::channel::<u64>(1);
            let (pong_sender, mut pong_receiver) = mpsc::channel::<()>(1);
            let traced = async move {
                for (count, value) in samples.iter().cloned() {
                    for _ in 0..count {
                        let span = info_span!("first").with_main_timings(example.clone());
                        async {
                            ping_sender.send(value as u64).await.unwrap();
                            pong_receiver.recv().await.unwrap();
                        }
                        .instrument(span)
                        .await;
                    }
                }
            }
            .boxed();
            let responder = async {
                while let Some(duration) = ping_receiver.recv().await {
                    tctx.mock.increment(Duration::from_secs(duration));
                    pong_sender.send(()).await.unwrap();
                }
            }
            .boxed();
            executor::block_on(join_all(vec![traced, responder]));
        });

        let data_points = tctx.extract_data_points();
        assert_eq!(data_points.len(), 2);
        // test asserts that both wait and total timings have same value as
        // future spends all the time in await
        for data_point in &data_points {
            assert_eq!(
                data_point.count,
                samples.iter().map(|(count, _)| *count as u64).sum::<u64>()
            );
            assert_eq!(
                data_point.sum,
                samples
                    .iter()
                    .map(|(count, value)| *count as f64 * *value as f64)
                    .sum::<f64>()
            );
        }
    }
}
