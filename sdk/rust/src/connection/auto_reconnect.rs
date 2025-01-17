use std::future::Future;

use asteroid_mq_model::{
    connection::{EdgeConnectionError, EdgeNodeConnection},
    EdgePayload,
};
use futures_util::{ready, Sink, Stream};

pub trait ReconnectableConnection: Sized {
    type ReconnectFuture: Future<Output = Result<Self, EdgeConnectionError>> + Send;
    fn reconnect(&self) -> Self::ReconnectFuture;
    fn is_closed(&self) -> bool;
}

pub trait ReconnectableConnectionExt: Sized
where
    Self: EdgeNodeConnection + ReconnectableConnection,
{
    fn auto_reconnect(self) -> AutoReconnect<Self, Self::ReconnectFuture>;
}

impl<C> ReconnectableConnectionExt for C
where
    C: EdgeNodeConnection + ReconnectableConnection,
{
    fn auto_reconnect(self) -> AutoReconnect<Self, Self::ReconnectFuture> {
        AutoReconnect::new(self)
    }
}

pin_project_lite::pin_project! {
    pub struct AutoReconnect<C, RF> {
        #[pin]
        connection: C,
        #[pin]
        reconnect_future: Option<RF>,
    }
}
impl<C> AutoReconnect<C, C::ReconnectFuture>
where
    C: EdgeNodeConnection + ReconnectableConnection,
{
    pub fn new(connection: C) -> Self {
        Self {
            connection,
            reconnect_future: None,
        }
    }
}

impl<C> Sink<EdgePayload> for AutoReconnect<C, C::ReconnectFuture>
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
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        let mut this = self.project();

        // check if it is reconnecting
        if let Some(reconnect_future) = this.reconnect_future.as_mut().as_pin_mut() {
            let new_connection = ready!(reconnect_future.poll(cx)?);
            this.connection.set(new_connection);
            this.reconnect_future.set(None)
        }

        // if the connection is not ready, try to reconnect
        if this.connection.is_closed() {
            let reconnect_future = this.connection.reconnect();
            this.reconnect_future.set(Some(reconnect_future));
            return std::task::Poll::Pending;
        }
        this.connection.poll_ready(cx)
    }
    fn start_send(self: std::pin::Pin<&mut Self>, item: EdgePayload) -> Result<(), Self::Error> {
        self.project().connection.start_send(item)
    }
}

impl<C> Stream for AutoReconnect<C, C::ReconnectFuture>
where
    C: EdgeNodeConnection + ReconnectableConnection,
{
    type Item = C::Item;
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let mut this = self.project();

        // check if it is reconnecting
        if let Some(reconnect_future) = this.reconnect_future.as_mut().as_pin_mut() {
            let new_connection = ready!(reconnect_future.poll(cx)?);
            this.connection.set(new_connection);
            this.reconnect_future.set(None)
        }

        // if the connection is not ready, try to reconnect
        if this.connection.is_closed() {
            let reconnect_future = this.connection.reconnect();
            this.reconnect_future.set(Some(reconnect_future));
            return std::task::Poll::Pending;
        }
        this.connection.poll_next(cx)
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.connection.size_hint()
    }
}
impl<C> EdgeNodeConnection for AutoReconnect<C, C::ReconnectFuture> where
    C: EdgeNodeConnection + ReconnectableConnection
{
}
