use std::task::ready;

use bytes::Bytes;
use futures_util::{Sink, Stream};
use tokio_tungstenite::{tungstenite::Message, WebSocketStream};

use crate::protocol::node::event::N2nPacket;

use super::{NodeConnection, NodeConnectionError, NodeConnectionErrorKind};

pin_project_lite::pin_project! {
    #[derive(Debug)]
    pub struct TokioWs {
        #[pin]
        inner: WebSocketStream<tokio::net::TcpStream>,
    }
}
impl TokioWs {
    pub fn new(inner: WebSocketStream<tokio::net::TcpStream>) -> Self {
        Self { inner }
    }
    pub async fn from_tokio_tcp_stream(
        inner: tokio::net::TcpStream,
    ) -> Result<Self, tokio_tungstenite::tungstenite::Error> {
        let ws_stream = tokio_tungstenite::accept_async(inner).await?;
        Ok(Self::new(ws_stream))
    }
}
impl Sink<N2nPacket> for TokioWs {
    type Error = NodeConnectionError;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.project().inner.poll_ready(cx).map_err(|e| {
            NodeConnectionError::new(
                NodeConnectionErrorKind::Underlying(Box::new(e)),
                "web socket poll ready failed",
            )
        })
    }

    fn start_send(self: std::pin::Pin<&mut Self>, item: N2nPacket) -> Result<(), Self::Error> {
        self.project()
            .inner
            .start_send(Message::binary(item.to_binary()))
            .map_err(|e| {
                NodeConnectionError::new(
                    NodeConnectionErrorKind::Underlying(Box::new(e)),
                    "web socket start send failed",
                )
            })
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.project().inner.poll_flush(cx).map_err(|e| {
            NodeConnectionError::new(
                NodeConnectionErrorKind::Underlying(Box::new(e)),
                "web socket poll flush failed",
            )
        })
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.project().inner.poll_close(cx).map_err(|e| {
            NodeConnectionError::new(
                NodeConnectionErrorKind::Underlying(Box::new(e)),
                "web socket poll close failed",
            )
        })
    }
}

impl Stream for TokioWs {
    type Item = Result<N2nPacket, NodeConnectionError>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let next = ready!(self.project().inner.poll_next(cx));
        match next {
            Some(Ok(Message::Binary(data))) => {
                let data = Bytes::from(data);
                let packet = N2nPacket::from_binary(data).map_err(|e| {
                    NodeConnectionError::new(
                        NodeConnectionErrorKind::Decode(e),
                        "invalid binary packet",
                    )
                });
                std::task::Poll::Ready(Some(packet))
            }
            Some(Ok(p)) => {
                tracing::debug!(?p, "unexpected message type");
                // immediately wake up the task to poll next
                cx.waker().wake_by_ref();
                std::task::Poll::Pending
            }
            Some(Err(e)) => std::task::Poll::Ready(Some(Err(NodeConnectionError::new(
                NodeConnectionErrorKind::Underlying(Box::new(e)),
                "web socket poll next failed",
            )))),
            None => std::task::Poll::Ready(None),
        }
    }
}

impl NodeConnection for TokioWs {}
