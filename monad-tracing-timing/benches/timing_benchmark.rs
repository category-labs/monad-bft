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

use criterion::{criterion_group, criterion_main, Criterion};
use futures::executor;
use monad_tracing_timing::{TimingSpanExtension, TimingsLayer};
use opentelemetry::metrics::MeterProvider;
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
use tracing::{info_span, Instrument};
use tracing_subscriber::{layer::SubscriberExt, Registry};

fn bench_spans(c: &mut Criterion) {
    let provider = SdkMeterProvider::builder()
        .with_reader(PeriodicReader::builder(InMemoryMetricExporter::default()).build())
        .build();

    let histogram = provider.meter("benchmark").f64_histogram("example").build();

    let timings = TimingsLayer::new();
    let subscriber = Registry::default().with(timings);

    tracing::subscriber::with_default(subscriber, || {
        c.bench_function("sync", |b| {
            b.iter(|| {
                let main_span = info_span!("main_span").with_main_timings(histogram.clone());
                let _main_guard = main_span.enter();

                let secondary_span = info_span!("secondary_span").with_sub_timings();
                let _secondary_guard = secondary_span.enter();
            });
        });

        c.bench_function("async_spans", |b| {
            b.iter(|| {
                let task = async {
                    let secondary_fut = async {
                        std::hint::black_box(());
                    };

                    let main_fut = async {
                        let secondary_span = info_span!("secondary_async_span").with_sub_timings();
                        secondary_fut.instrument(secondary_span).await;
                    };

                    let main_span =
                        info_span!("main_async_span").with_main_timings(histogram.clone());
                    main_fut.instrument(main_span).await;
                };

                executor::block_on(task);
            });
        });
    });
}

criterion_group!(benches, bench_spans);
criterion_main!(benches);
