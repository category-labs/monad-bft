use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::Stream;

pub struct MockEpoch<E> {
    state: Option<E>,
}

impl<E> MockEpoch<E> {
    pub fn ready(&self) -> bool {
        self.state.is_some()
    }
}

impl<E> Default for MockEpoch<E> {
    fn default() -> Self {
        Self { state: None }
    }
}

impl<E> Stream for MockEpoch<E>
where
    Self: Unpin,
{
    type Item = E;
    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Pending
    }
}
