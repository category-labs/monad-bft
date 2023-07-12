use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::Stream;

pub struct MockStateUpdate<E> {
    state: Option<E>,
}

impl<E> MockStateUpdate<E> {
    pub fn ready(&self) -> bool {
        self.state.is_some()
    }
}

impl<E> Default for MockStateUpdate<E> {
    fn default() -> Self {
        Self { state: None }
    }
}

impl<E> Stream for MockStateUpdate<E>
where
    Self: Unpin,
{
    type Item = E;
    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Pending
    }
}
