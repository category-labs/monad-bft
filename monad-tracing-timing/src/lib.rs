use std::{any::TypeId, cell::RefCell};

use opentelemetry::{metrics::Histogram, KeyValue};
use tracing::{span::Id, Dispatch, Span, Subscriber};
use tracing_subscriber::{layer::Context, registry::LookupSpan, Layer};

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
    histogram: Histogram<f64>,
}

pub struct TimingsLayer<S> {
    clock: quanta::Clock,
    main_span_callback: MainSpanCallback,
    metered_span_callback: MeteredSpanCallback,
    _phantom: std::marker::PhantomData<S>,
}

struct MainSpanCallback(fn(&Dispatch, &Id, Histogram<f64>));

struct MeteredSpanCallback(fn(&Dispatch, &Id));

impl<S> TimingsLayer<S>
where
    S: Subscriber + for<'span> LookupSpan<'span>,
{
    pub fn new() -> Self {
        Self {
            clock: quanta::Clock::new(),
            main_span_callback: MainSpanCallback(Self::update_main_span),
            metered_span_callback: MeteredSpanCallback(Self::update_metered_span),
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

    fn update_main_span(d: &Dispatch, id: &Id, histogram: Histogram<f64>) {
        let span = d.downcast_ref::<S>().unwrap().span(id).unwrap();
        span.extensions_mut().insert(TimingContext {
            span_type: SpanType::Main,
            entered: None,
            histogram,
        });
    }

    fn update_metered_span(d: &Dispatch, id: &Id) {
        let subscriber = d.downcast_ref::<S>().unwrap();
        let main_span = MAIN_SPAN.with(|span| {
            let id = span.borrow();
            subscriber.span(&id)
        });
        if let Some(main_span) = main_span {
            let span = subscriber.span(id).unwrap();
            span.extensions_mut().insert(TimingContext {
                span_type: SpanType::Secondary(main_span.id()),
                entered: None,
                histogram: main_span
                    .extensions()
                    .get::<TimingContext>()
                    .unwrap()
                    .histogram
                    .clone(),
            });
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
            id if id == TypeId::of::<MainSpanCallback>() => {
                Some(&self.main_span_callback as *const _ as *const ())
            }
            id if id == TypeId::of::<MeteredSpanCallback>() => {
                Some(&self.metered_span_callback as *const _ as *const ())
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
            // if it is main push it to the thread local storage on every enter
            if matches!(timing_context.span_type, SpanType::Main) {
                MAIN_SPAN.replace(id.clone());
            }
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let span = ctx.span(&id).expect("span must exist");
        let mut extensions = span.extensions_mut();
        if let Some(timing) = extensions.get_mut::<TimingContext>() {
            if let Some(entered) = timing.entered.take() {
                let elapsed = (self.clock.now() - entered).as_secs_f64();
                match &timing.span_type {
                    SpanType::Secondary(main) => {
                        if let Some(main_span) = ctx.span(main) {
                            timing.histogram.record(
                                elapsed,
                                &[
                                    KeyValue::new("main", main_span.name()),
                                    KeyValue::new("secondary", span.name()),
                                ],
                            );
                        } else {
                            eprintln!("main span not found");
                        }
                    }
                    SpanType::Main => {
                        timing
                            .histogram
                            .record(elapsed, &[KeyValue::new("main", span.name())]);
                    }
                }
            }
        }
    }
}

pub trait TimingSpanExtension {
    fn with_histogram(&self, histogram: Histogram<f64>) -> &Self;
    fn with_timings(&self) -> &Self;
}

impl TimingSpanExtension for Span {
    // with_histogram enables this span to report total time spent to this histogram.
    fn with_histogram(&self, histogram: Histogram<f64>) -> &Self {
        self.with_subscriber(|(id, subscriber)| {
            if let Some(callback) = subscriber.downcast_ref::<MainSpanCallback>() {
                callback.0(subscriber, id, histogram)
            }
        });
        self
    }

    fn with_timings(&self) -> &Self {
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

    use opentelemetry::{metrics::MeterProvider, KeyValue};
    use opentelemetry_sdk::metrics::{
        data::{Histogram as HistogramData, HistogramDataPoint},
        InMemoryMetricExporter, PeriodicReader, SdkMeterProvider,
    };
    use tracing::info_span;
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
            let histogram = finished.iter().next().expect("histogram must be there");
            let metric = histogram
                .scope_metrics
                .iter()
                .next()
                .expect("exactly 1 scope")
                .metrics
                .iter()
                .next()
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
                    let span = info_span!("first");
                    span.with_histogram(example.clone());
                    let _ = span.enter();
                    tctx.mock.increment(Duration::from_secs(value as u64));
                });
            }
        });

        let data_points = tctx.extract_data_points();
        assert_eq!(data_points.len(), 1);
        let data_point = &data_points[0];
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
        // skip 1st bucket as this is one below min value
        assert_eq!(
            data_point.bucket_counts[1..]
                .iter()
                .cloned()
                .collect::<Vec<_>>(),
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
                    let span = info_span!("first");
                    span.with_histogram(example.clone());
                    let _enter = span.enter();
                    tctx.mock.increment(Duration::from_secs(first));
                    let span = info_span!("second");
                    span.with_timings();
                    let _enter = span.enter();
                    tctx.mock.increment(Duration::from_secs(second));
                }
            }
        });

        let mut data_points = tctx.extract_data_points();
        assert_eq!(data_points.len(), 2);
        // NOTE(dshulyak) order is only for assertions
        data_points.sort_by_key(|k| k.sum as u64);
        assert_eq!(
            data_points[1].attributes,
            vec![KeyValue::new("main", "first")]
        );
        assert_eq!(
            data_points[0].attributes,
            vec![
                KeyValue::new("main", "first"),
                KeyValue::new("secondary", "second")
            ]
        );

        assert_eq!(data_points[1].bucket_counts, expected_buckets_first);
        assert_eq!(data_points[0].bucket_counts, expected_buckets_second);
    }
}
