use asteroid_mq_model::codec::Json;
use asteroid_mq_model::connection::EdgeNodeConnection;
use asteroid_mq_model::{
    codec::{Bincode, Codec},
    connection::EdgeConnectionError,
    EdgePayload,
};
use futures_util::stream::FusedStream;
use futures_util::{Sink, Stream};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::http::Request;
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::{ClientNode, ClientNodeError};

use super::auto_reconnect::{ReconnectableConnection, ReconnectableConnectionExt};

pin_project_lite::pin_project! {
    #[derive(Debug)]
    pub struct Ws2Client<C = Bincode> {
        #[pin]
        inner: WebSocketStream<MaybeTlsStream<TcpStream>>,
        codec: C,
        request: Request<()>,
    }
}

impl<C> Ws2Client<C>
where
    C: Codec,
{
    pub fn new(
        inner: WebSocketStream<MaybeTlsStream<TcpStream>>,
        request: Request<()>,
        codec: C,
    ) -> Self {
        Self {
            inner,
            codec,
            request,
        }
    }
    pub fn with_codec<C2>(self, codec: C2) -> Ws2Client<C2>
    where
        C2: Codec,
    {
        Ws2Client {
            inner: self.inner,
            request: self.request,
            codec,
        }
    }
}

impl<C: Codec> Ws2Client<C> {
    pub async fn create_by_request<R>(request: R, codec: C) -> Result<Self, EdgeConnectionError>
    where
        R: IntoClientRequest + Unpin,
    {
        let request = request
            .into_client_request()
            .map_err(EdgeConnectionError::underlying("ws create_by_request"))?;
        let (stream, _resp) = tokio_tungstenite::connect_async(request.clone())
            .await
            .map_err(EdgeConnectionError::underlying("ws create_by_request"))?;
        Ok(Self::new(stream, request, codec))
    }
}

impl<C> Stream for Ws2Client<C>
where
    C: Codec,
{
    type Item = Result<EdgePayload, EdgeConnectionError>;
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();
        let message = futures_util::ready!(this
            .inner
            .poll_next(cx)
            .map_err(EdgeConnectionError::underlying("ws poll_next"))?);
        let Some(message) = message else {
            return std::task::Poll::Ready(None);
        };
        let Message::Binary(payload) = message else {
            // skip
            return std::task::Poll::Pending;
        };

        let payload = this
            .codec
            .decode(&payload)
            .map_err(EdgeConnectionError::codec("ws poll_next"))?;
        std::task::Poll::Ready(Some(Ok(payload)))
    }
}
impl<C> Sink<EdgePayload> for Ws2Client<C>
where
    C: Codec,
{
    type Error = EdgeConnectionError;
    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        let this = self.project();
        this.inner
            .poll_close(cx)
            .map_err(EdgeConnectionError::underlying("ws poll_close"))
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        let this = self.project();
        this.inner
            .poll_flush(cx)
            .map_err(EdgeConnectionError::underlying("ws poll_flush"))
    }
    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        let this = self.project();
        this.inner
            .poll_ready(cx)
            .map_err(EdgeConnectionError::underlying("ws poll_ready"))
    }
    fn start_send(self: std::pin::Pin<&mut Self>, item: EdgePayload) -> Result<(), Self::Error> {
        let this = self.project();
        let payload = this
            .codec
            .encode(&item)
            .map_err(EdgeConnectionError::codec("ws start_send"))?;
        this.inner
            .start_send(tokio_tungstenite::tungstenite::Message::Binary(payload))
            .map_err(EdgeConnectionError::underlying("ws start_send"))?;
        Ok(())
    }
}

impl<C> EdgeNodeConnection for Ws2Client<C> where C: Codec {}

impl ClientNode {
    pub async fn connect_ws2<R: IntoClientRequest + Unpin>(
        req: R,
        codec: impl Codec + Clone,
    ) -> Result<ClientNode, ClientNodeError> {
        let client = Ws2Client::create_by_request(req, codec)
            .await?
            .auto_reconnect();
        let node = ClientNode::connect(client).await?;
        Ok(node)
    }
    pub async fn connect_ws2_bincode<R: IntoClientRequest + Unpin>(
        req: R,
    ) -> Result<ClientNode, ClientNodeError> {
        ClientNode::connect_ws2(req, Bincode).await
    }
    pub async fn connect_ws2_json<R: IntoClientRequest + Unpin>(
        req: R,
    ) -> Result<ClientNode, ClientNodeError> {
        ClientNode::connect_ws2(req, Json).await
    }
}

impl<C: Codec + Clone> ReconnectableConnection for Ws2Client<C> {
    type ReconnectFuture = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self, EdgeConnectionError>> + Send>,
    >;
    type SleepFuture = tokio::time::Sleep;
    fn is_closed(&self) -> bool {
        self.inner.is_terminated()
    }
    fn reconnect(&self) -> Self::ReconnectFuture {
        let request = self.request.clone();
        let codec = self.codec.clone();
        Box::pin(async move {
            let client = Ws2Client::create_by_request(request, codec).await?;
            Ok(client)
        })
    }
    fn sleep(&self, duration: std::time::Duration) -> Self::SleepFuture {
        tokio::time::sleep(duration)
    }
}
