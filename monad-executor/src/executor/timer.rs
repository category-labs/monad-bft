use std::{
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use futures::{FutureExt, Stream};

use crate::{Executor, TimerCommand};

pub struct TokioTimer<E> {
    state: Option<(Pin<Box<tokio::time::Sleep>>, E)>,
    waker: Option<Waker>,
}
impl<E> Default for TokioTimer<E> {
    fn default() -> Self {
        Self {
            state: None,
            waker: None,
        }
    }
}
impl<E> Executor for TokioTimer<E> {
    type Command = TimerCommand<E>;
    fn exec(&mut self, commands: Vec<TimerCommand<E>>) {
        let mut wake = false;
        for command in commands {
            self.state = match command {
                TimerCommand::Schedule {
                    duration,
                    on_timeout,
                } => {
                    wake = true;
                    Some((Box::pin(tokio::time::sleep(duration)), on_timeout))
                }
                TimerCommand::ScheduleReset => {
                    wake = false;
                    None
                }
            }
        }

        if wake {
            if let Some(waker) = self.waker.take() {
                waker.wake();
            }
        }
    }
}
impl<E> Stream for TokioTimer<E>
where
    Self: Unpin,
{
    type Item = E;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        if let Some((sleep, _)) = this.state.as_mut() {
            if sleep.poll_unpin(cx).is_ready() {
                return Poll::Ready(Some(this.state.take().expect("event must exist").1));
            }
        }

        self.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::StreamExt;

    use super::*;

    #[tokio::test]
    async fn test_schedule() {
        let mut timer = TokioTimer::default();
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);

        timer.exec(vec![TimerCommand::Schedule {
            duration: Duration::from_millis(0),
            on_timeout: (),
        }]);

        assert_eq!(timer.next().await, Some(()));
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);
    }

    #[tokio::test]
    async fn test_double_schedule() {
        let mut timer = TokioTimer::default();
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);

        timer.exec(vec![TimerCommand::Schedule {
            duration: Duration::from_millis(0),
            on_timeout: (),
        }]);
        timer.exec(vec![TimerCommand::Schedule {
            duration: Duration::from_millis(0),
            on_timeout: (),
        }]);

        assert_eq!(timer.next().await, Some(()));
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);
    }

    #[tokio::test]
    async fn test_reset() {
        let mut timer = TokioTimer::default();
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);

        timer.exec(vec![TimerCommand::Schedule {
            duration: Duration::from_millis(0),
            on_timeout: (),
        }]);
        timer.exec(vec![TimerCommand::ScheduleReset]);

        assert_eq!(futures::poll!(timer.next()), Poll::Pending);
    }

    #[tokio::test]
    async fn test_inline_double_schedule() {
        let mut timer = TokioTimer::default();
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);

        timer.exec(vec![
            TimerCommand::Schedule {
                duration: Duration::from_millis(0),
                on_timeout: (),
            },
            TimerCommand::Schedule {
                duration: Duration::from_millis(0),
                on_timeout: (),
            },
        ]);

        assert_eq!(timer.next().await, Some(()));
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);
    }

    #[tokio::test]
    async fn test_inline_reset() {
        let mut timer = TokioTimer::default();
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);

        timer.exec(vec![
            TimerCommand::Schedule {
                duration: Duration::from_millis(0),
                on_timeout: (),
            },
            TimerCommand::ScheduleReset,
        ]);

        assert_eq!(futures::poll!(timer.next()), Poll::Pending);
    }

    #[tokio::test]
    async fn test_inline_reset_schedule() {
        let mut timer = TokioTimer::default();
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);

        timer.exec(vec![
            TimerCommand::ScheduleReset,
            TimerCommand::Schedule {
                duration: Duration::from_millis(0),
                on_timeout: (),
            },
        ]);

        assert_eq!(timer.next().await, Some(()));
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);
    }

    #[tokio::test]
    async fn test_noop_exec() {
        let mut timer = TokioTimer::default();
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);

        timer.exec(vec![TimerCommand::Schedule {
            duration: Duration::from_millis(1),
            on_timeout: (),
        }]);
        timer.exec(Vec::new());

        assert_eq!(timer.next().await, Some(()));
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);
    }
}
