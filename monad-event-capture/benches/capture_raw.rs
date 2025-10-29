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

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use monad_event::BytesDecoder;
use monad_event_capture::{
    EventCaptureEventIter, EventCaptureFile, EventCaptureNextResult, EventCaptureSectionType,
};

fn bench_snapshot(c: &mut Criterion) {
    const CAPTURE_PATH: &str = "../monad-event/test/data/exec-events-emn-30b-15m/capture.evcap";

    let mut g = c.benchmark_group("capture_raw");

    let event_capture_file = EventCaptureFile::open(CAPTURE_PATH).unwrap();

    let items = {
        let mut event_capture_reader = event_capture_file.create_reader().unwrap();

        let mut items = 0;

        while let Some(section_descriptor) =
            event_capture_reader.next_section(Some(EventCaptureSectionType::EventBundle))
        {
            let event_section = section_descriptor.open_event_section().unwrap();

            let mut event_iter: EventCaptureEventIter<'_, BytesDecoder> =
                event_section.open_iterator();

            loop {
                match event_iter.next_descriptor() {
                    EventCaptureNextResult::Success(_) => {
                        items += 1;
                    }
                    EventCaptureNextResult::End => break,
                    EventCaptureNextResult::NoSeqno => unreachable!(),
                }
            }
        }

        items
    };

    g.bench_function("reader_create_drop", |b| {
        b.iter(|| {
            black_box(event_capture_file.create_reader().unwrap());
        });
    });

    g.throughput(criterion::Throughput::Elements(items));
    g.bench_function("iter", |b| {
        b.iter_batched_ref(
            || event_capture_file.create_reader().unwrap(),
            |event_capture_reader| {
                while let Some(section_descriptor) =
                    event_capture_reader.next_section(Some(EventCaptureSectionType::EventBundle))
                {
                    let event_section = section_descriptor.open_event_section().unwrap();

                    let mut event_iter: EventCaptureEventIter<'_, BytesDecoder> =
                        event_section.open_iterator();

                    loop {
                        match event_iter.next_descriptor() {
                            EventCaptureNextResult::Success(event_descriptor) => {
                                let actual_payload: Option<u8> = event_descriptor
                                    .try_filter_map_raw(|_, bytes| {
                                        black_box(Some(bytes.first().cloned().unwrap_or_default()))
                                    });

                                black_box(actual_payload);
                            }
                            EventCaptureNextResult::End => break,
                            EventCaptureNextResult::NoSeqno => unreachable!(),
                        }
                    }
                }
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, bench_snapshot);
criterion_main!(benches);
