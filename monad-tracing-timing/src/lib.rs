use std::{any::TypeId, cell::RefCell, sync::Arc};

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
    Metered,
}

#[derive(Debug)]
struct TimingContext {
    span_type: SpanType,
    // recorded when span entered, cleared on close
    // note that it is important not to clear it on _exit_, as then we will capture await times
    // rather then whole duration
    entered: Option<quanta::Instant>,
    // TODO get rid of this, generally i want next things
    // - label from top level span
    // - label from 1 currently tracked span
    // - any additional labels
    main_label: &'static str,
    histogram: Arc<Histogram<f64>>,
}

pub struct TimingsLayer<S> {
    clock: quanta::Clock,
    main_span_callback: MainSpanCallback,
    metered_span_callback: MeteredSpanCallback,
    _phantom: std::marker::PhantomData<S>,
}

struct MainSpanCallback(fn(&Dispatch, &Id, Arc<Histogram<f64>>));

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

    fn update_main_span(d: &Dispatch, id: &Id, histogram: Arc<Histogram<f64>>) {
        let span = d.downcast_ref::<S>().unwrap().span(id).unwrap();
        span.extensions_mut().insert(TimingContext {
            span_type: SpanType::Main,
            entered: None,
            main_label: "main",
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
                span_type: SpanType::Metered,
                entered: None,
                main_label: main_span
                    .extensions()
                    .get::<TimingContext>()
                    .unwrap()
                    .main_label,
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
                let elapsed = entered.elapsed().as_secs_f64();
                let request = KeyValue::new("request", timing.main_label);
                match timing.span_type {
                    SpanType::Metered => timing
                        .histogram
                        .record(elapsed, &[request, KeyValue::new("step", span.name())]),
                    SpanType::Main => timing.histogram.record(elapsed, &[request]),
                }
            }
        }
    }
}

pub trait TimingSpanExtension {
    fn with_histogram(&self, histogram: Arc<Histogram<f64>>) -> &Self;
    fn with_timings(&self) -> &Self;
}

impl TimingSpanExtension for Span {
    // with_histogram enables this span to report total time spent to this histogram.
    fn with_histogram(&self, histogram: Arc<Histogram<f64>>) -> &Self {
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
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_sdk::metrics::{
        InMemoryMetricExporter, ManualReader, PeriodicReader, PeriodicReaderBuilder,
        SdkMeterProvider,
    };

    use std::{thread::sleep, time::Duration};

    use tracing::info_span;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::Registry;

    use crate::{TimingSpanExtension, TimingsLayer};

    #[test]
    fn test_sanity() {
        let exporter = InMemoryMetricExporter::default();
        let reader = PeriodicReader::builder(exporter.clone()).build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();

        let layer = TimingsLayer::new();
        let subscriber = Registry::default().with(layer);
        tracing::subscriber::set_global_default(subscriber)
            .expect("Failed to set global default subscriber");

        let meter = provider.meter("test");
        let example_hist = meter.f64_histogram("example").with_unit("s").init();
        let example_hist = std::sync::Arc::new(example_hist);

        let span = info_span!("dbg");
        span.with_histogram(example_hist);
        span.in_scope(|| {
            sleep(Duration::from_millis(10)); // TODO move the clock instead
        });
        provider.force_flush();
        for resource_metrics in finished_metrics {
            println!("{:?}", resource_metrics);
        }
    }
}
