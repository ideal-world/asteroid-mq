use std::{future::Future, task::{Poll, Waker}};

use asteroid_mq_model::{
    connection::{EdgeConnectionError, EdgeNodeConnection},
    EdgePayload,
};
use futures_util::{ready, Sink, Stream};
use std::time::Duration;

pub trait ReconnectableConnection: Sized {
    type ReconnectFuture: Future<Output = Result<Self, EdgeConnectionError>> + Send;
    type SleepFuture: Future<Output = ()> + Send;
    fn reconnect(&self) -> Self::ReconnectFuture;
    fn sleep(&self, duration: Duration) -> Self::SleepFuture;
    fn is_closed(&self) -> bool;
}

pub trait ReconnectableConnectionExt: Sized
where
    Self: EdgeNodeConnection + ReconnectableConnection,
{
    fn auto_reconnect(self) -> AutoReconnect<Self> {
        Self::auto_reconnect_with_config(self, Default::default())
    }
    fn auto_reconnect_with_config(self, config: ReconnectConfig) -> AutoReconnect<Self>;
}

impl<C> ReconnectableConnectionExt for C
where
    C: EdgeNodeConnection + ReconnectableConnection,
{
    fn auto_reconnect_with_config(self, config: ReconnectConfig) -> AutoReconnect<Self> {
        AutoReconnect::new_with_config(self, config)
    }
}
#[derive(Clone, Debug)]
pub struct ReconnectConfig {
    pub max_failure_times: Option<u64>,
    pub retry_interval: Duration,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            max_failure_times: None,
            retry_interval: Duration::from_secs(1),
        }
    }
}
pin_project_lite::pin_project! {
    #[project = ReconnectStatusProj]
    pub enum ReconnectStatus<R, S> {
        Reconnecting {
            #[pin]
            future: R,
            wakers: Vec<Waker>,
        },
        Sleeping {
            #[pin]
            future: S,
            wakers: Vec<Waker>,
        },
        Connected,
    }
}
pin_project_lite::pin_project! {
    pub struct AutoReconnect<C: ReconnectableConnection> {
        #[pin]
        connection: C,
        #[pin]
        reconnect_status: ReconnectStatus<C::ReconnectFuture, C::SleepFuture>,
        pub reconnect_config: ReconnectConfig,
        retry_times: u64,
        reconnected_times: u64,
        // reconnected signal
        just_reconnected: u64,
    }
}

impl<C> AutoReconnect<C>
where
    C: EdgeNodeConnection + ReconnectableConnection,
{
    pub fn new(connection: C) -> Self {
        Self::new_with_config(connection, Default::default())
    }

    pub fn new_with_config(connection: C, reconnect_config: ReconnectConfig) -> Self {
        Self {
            connection,
            reconnect_status: ReconnectStatus::Connected,
            reconnect_config,
            retry_times: 0,
            reconnected_times: 0,
            just_reconnected: 0,
        }
    }
    pub fn poll_connection_ready(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), C::Error>> {
        let mut this = self.as_mut().project();
        let status = this.reconnect_status.as_mut().project();
        match status {
            ReconnectStatusProj::Reconnecting { future, wakers } => {
                wakers.push(cx.waker().clone());
                Poll::Pending
            },
            ReconnectStatusProj::Sleeping { future, wakers } => {
                wakers.push(cx.waker().clone());
                Poll::Pending
            },
            ReconnectStatusProj::Connected => {
                if this.connection.is_closed() {
                    tracing::warn!("connection is not ready, reconnecting");       
                    let reconnect_future = this.connection.reconnect();
                    this.reconnect_status.set(ReconnectStatus::Reconnecting {
                        future: reconnect_future,
                        wakers: vec![cx.waker().clone()],
                    });
                    Poll::Pending
                } else {
                    this.connection.as_mut().poll_ready(cx)
                }
            }
        }
    }
    pub fn poll_reconnecting(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), C::Error>> {
        let mut this = self.project();
        loop {
            let status = this.reconnect_status.as_mut().project();
            match status {
                ReconnectStatusProj::Reconnecting { future, wakers } => {
                    let reconnect_result = ready!(future.poll(cx));
                    match reconnect_result {
                        Err(e) => {
                            tracing::error!(error = ?e, "reconnect failed");
                            if let Some(max_failure_times) = this.reconnect_config.max_failure_times
                            {
                                if *this.retry_times >= max_failure_times {
                                    // if the retry times exceed the max failure times, return the reconnect error
                                    return std::task::Poll::Ready(Err(e));
                                }
                            }
                            *this.retry_times += 1;
                            let wakers = wakers.clone();
                            this.reconnect_status.set(ReconnectStatus::Sleeping {
                                future: this.connection.sleep(this.reconnect_config.retry_interval),
                                wakers,
                            });
                            continue;
                        }
                        Ok(new_connection) => {
                            let retry_times = *this.retry_times;
                            let reconnected_times = *this.reconnected_times;
                            tracing::info!(retry_times, reconnected_times, wakers_count = wakers.len(), "reconnect success");
                            for waker in wakers {
                                waker.wake_by_ref();
                            }
                            *this.retry_times = 0;
                            *this.reconnected_times += 1;
                            *this.just_reconnected += 1;
                            this.connection.set(new_connection);
                            this.reconnect_status.set(ReconnectStatus::Connected);
                            continue;
                        }
                    }
                }
                ReconnectStatusProj::Sleeping { future, wakers } => {
                    ready!(future.poll(cx));
                    let wakers = wakers.clone();
                    let reconnect_future = this.connection.reconnect();
                    this.reconnect_status.set(ReconnectStatus::Reconnecting {
                        future: reconnect_future,
                        wakers
                    });
                    continue;
                }
                ReconnectStatusProj::Connected => {
                    if this.connection.is_closed() {
                        tracing::warn!("connection closed, reconnecting");
                        let reconnect_future = this.connection.reconnect();
                        this.reconnect_status.set(ReconnectStatus::Reconnecting {
                            future: reconnect_future,
                            wakers: vec![]
                        });
                        continue;
                    }
                    return std::task::Poll::Ready(Ok(()));
                }
            }
        }
    }
}

impl<C> Sink<EdgePayload> for AutoReconnect<C>
where
    C: EdgeNodeConnection + ReconnectableConnection,
{
    type Error = C::Error;
    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.project().connection.poll_close(cx)
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.project().connection.poll_flush(cx)
    }
    fn poll_ready(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        // check if it is reconnecting
        self.as_mut().poll_connection_ready(cx)
    }
    fn start_send(self: std::pin::Pin<&mut Self>, item: EdgePayload) -> Result<(), Self::Error> {
        tracing::warn!(?item, "[debug] ar payload do send");
        self.project().connection.start_send(item)
    }
}

impl<C> Stream for AutoReconnect<C>
where
    C: EdgeNodeConnection + ReconnectableConnection,
{
    type Item = C::Item;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        // check if it is reconnecting
        ready!(self.as_mut().poll_reconnecting(cx)?);
        let mut this = self.as_mut().project();
        if *this.just_reconnected != 0 {
            // consume this reconnected signal
            *this.just_reconnected -= 1;
            // if the retry times is increased, it means the connection is closed
            tracing::warn!("sending just reconnect error");
            cx.waker().wake_by_ref();
            return std::task::Poll::Ready(Some(Err(EdgeConnectionError::new(
                asteroid_mq_model::connection::EdgeConnectionErrorKind::Reconnect,
                "poll_next",
            ))));
        }
        let poll_next_result = ready!(this.connection.as_mut().poll_next(cx)?);
        if poll_next_result.is_some() {
            return std::task::Poll::Ready(poll_next_result.map(Ok));
        }
        // if the connection is closed, reconnect
        let reconnect_future = this.connection.reconnect();
        this.reconnect_status.set(ReconnectStatus::Reconnecting {
            future: reconnect_future,
            wakers: vec![]
        });
        self.poll_next(cx)
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.connection.size_hint()
    }
}

impl<C> EdgeNodeConnection for AutoReconnect<C> where C: EdgeNodeConnection + ReconnectableConnection
{}
