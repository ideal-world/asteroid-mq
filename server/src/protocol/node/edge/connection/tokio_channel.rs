use asteroid_mq_model::{
    connection::{EdgeConnectionError, EdgeConnectionErrorKind, EdgeNodeConnection},
    EdgePayload,
};
use futures_util::{Sink, Stream};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
impl TokioChannelSocket {
    pub fn pair() -> (TokioChannelSocket, TokioChannelSocket) {
        let (tx_a_to_b, rx_a_to_b) = unbounded_channel();
        let (tx_b_to_a, rx_b_to_a) = unbounded_channel();
        (
            TokioChannelSocket {
                tx: tx_a_to_b,
                rx: rx_b_to_a,
            },
            TokioChannelSocket {
                tx: tx_b_to_a,
                rx: rx_a_to_b,
            },
        )
    }
}

pin_project_lite::pin_project! {
    pub struct TokioChannelSocket {
        #[pin]
        tx: UnboundedSender<EdgePayload>,
        #[pin]
        rx: UnboundedReceiver<EdgePayload>,
    }
}

impl Stream for TokioChannelSocket {
    type Item = Result<EdgePayload, EdgeConnectionError>;
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let mut this = self.project();
        let message = futures_util::ready!(this.rx.poll_recv(cx));
        std::task::Poll::Ready(message.map(Ok))
    }
}

impl Sink<EdgePayload> for TokioChannelSocket {
    type Error = EdgeConnectionError;
    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn start_send(self: std::pin::Pin<&mut Self>, item: EdgePayload) -> Result<(), Self::Error> {
        let this = self.project();
        let send_result = this.tx.send(item);
        if send_result.is_err() {
            return Err(EdgeConnectionError::new(
                EdgeConnectionErrorKind::Closed,
                "tokio channel closed",
            ));
        }
        Ok(())
    }
}
impl EdgeNodeConnection for TokioChannelSocket {}
