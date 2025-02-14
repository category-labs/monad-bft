use std::{pin::Pin, sync::Arc, task::Poll};

use futures::{task::AtomicWaker, Stream};
use rtrb::{PopError, RingBuffer};

pub fn make_producer_consumer<T>(size: usize) -> (WakeableProducer<T>, WakeableConsumer<T>) {
    let (ing_send, ing_recv) = RingBuffer::<T>::new(size);
    let n = Notifier::new();
    let ing_producer = WakeableProducer {
        producer: ing_send,
        notify: n.clone(),
    };
    let ing_consumer = WakeableConsumer {
        consumer: ing_recv,
        notify: n,
    };
    (ing_producer, ing_consumer)
}

#[derive(Clone)]
pub struct Notifier(pub Arc<AtomicWaker>);

#[allow(clippy::new_without_default)]
impl Notifier {
    pub fn new() -> Self {
        Self(Arc::new(AtomicWaker::new()))
    }
}

// wraps the spsc consumer with a notifier to implement stream
pub struct WakeableConsumer<T> {
    pub consumer: rtrb::Consumer<T>,
    pub notify: Notifier,
}

impl<T> Stream for WakeableConsumer<T> {
    type Item = T;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.consumer.pop() {
            Ok(t) => {
                return Poll::Ready(Some(t));
            }
            Err(PopError::Empty) => {
                self.notify.0.register(cx.waker());
            }
        }
        // documentation for AtomicWaker shows example checking condition again after register
        // waker to "avoid race condition that would result in lost notification"
        match self.consumer.pop() {
            Ok(t) => Poll::Ready(Some(t)),
            Err(PopError::Empty) => Poll::Pending,
        }
    }
}

pub struct WakeableProducer<T> {
    pub producer: rtrb::Producer<T>,
    pub notify: Notifier,
}
