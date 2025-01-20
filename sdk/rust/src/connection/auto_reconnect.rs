use std::future::Future;

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
        },
        Sleeping {
            #[pin]
            future: S,
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
        }
    }

    pub fn poll_reconnecting(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), C::Error>> {
        let mut this = self.project();
        let status = this.reconnect_status.as_mut().project();
        match status {
            ReconnectStatusProj::Reconnecting { future } => {
                let reconnect_result = ready!(future.poll(cx));
                match reconnect_result {
                    Err(e) => {
                        tracing::error!(error = ?e, "reconnect failed");
                        if let Some(max_failure_times) = this.reconnect_config.max_failure_times {
                            if *this.retry_times >= max_failure_times {
                                // if the retry times exceed the max failure times, return the reconnect error
                                return std::task::Poll::Ready(Err(e));
                            }
                        }
                        *this.retry_times += 1;
                        this.reconnect_status.set(ReconnectStatus::Sleeping {
                            future: this.connection.sleep(this.reconnect_config.retry_interval),
                        });
                        std::task::Poll::Pending
                    }
                    Ok(new_connection) => {
                        *this.retry_times = 0;
                        this.connection.set(new_connection);
                        this.reconnect_status.set(ReconnectStatus::Connected);
                        std::task::Poll::Ready(Ok(()))
                    }
                }
            }
            ReconnectStatusProj::Sleeping { future } => {
                ready!(future.poll(cx));
                let reconnect_future = this.connection.reconnect();
                this.reconnect_status.set(ReconnectStatus::Reconnecting {
                    future: reconnect_future,
                });
                std::task::Poll::Pending
            }
            ReconnectStatusProj::Connected => {
                if this.connection.is_closed() {
                    let reconnect_future = this.connection.reconnect();
                    this.reconnect_status.set(ReconnectStatus::Reconnecting {
                        future: reconnect_future,
                    });
                    return std::task::Poll::Pending;
                }
                std::task::Poll::Ready(Ok(()))
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
        ready!(self.as_mut().poll_reconnecting(cx)?);
        let this = self.project();
        this.connection.poll_ready(cx)
    }
    fn start_send(self: std::pin::Pin<&mut Self>, item: EdgePayload) -> Result<(), Self::Error> {
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
        let this = self.project();
        this.connection.poll_next(cx)
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.connection.size_hint()
    }
}
impl<C> EdgeNodeConnection for AutoReconnect<C> where C: EdgeNodeConnection + ReconnectableConnection
{}
