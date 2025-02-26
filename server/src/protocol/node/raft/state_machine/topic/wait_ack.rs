use std::{
    collections::{HashMap, HashSet},
    future::Future,
    task::Poll,
};

use asteroid_mq_model::EndpointAddr;
pub use asteroid_mq_model::{WaitAckError, WaitAckErrorException, WaitAckResult, WaitAckSuccess};
use serde::{Deserialize, Serialize};

use crate::{protocol::message::*, util::FixedHash};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WaitAck {
    pub expect: MessageAckExpectKind,
    pub status: HashMap<EndpointAddr, MessageStatusKind, FixedHash>,
}

impl WaitAck {
    pub fn new(expect: MessageAckExpectKind, ep_list: HashSet<EndpointAddr>) -> Self {
        let status = HashMap::<EndpointAddr, MessageStatusKind, FixedHash>::from_iter(
            ep_list
                .into_iter()
                .map(|ep| (ep, MessageStatusKind::Unsent)),
        );

        Self { status, expect }
    }
}

pin_project_lite::pin_project! {
    pub struct WaitAckHandle {
        pub(crate) message_id: MessageId,
        #[pin]
        pub(crate) result: tokio::sync::oneshot::Receiver<WaitAckResult>,
    }

}

impl WaitAckHandle {
    pub fn message_id(&self) -> MessageId {
        self.message_id
    }
    pub fn new(id: MessageId) -> (AckSender, WaitAckHandle) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        (
            AckSender::new(id, tx),
            WaitAckHandle {
                message_id: id,
                result: rx,
            },
        )
    }
}

impl Future for WaitAckHandle {
    type Output = Result<WaitAckSuccess, WaitAckError>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        this.result
            .poll(cx)
            .map_err(|_| WaitAckError::exception(WaitAckErrorException::MessageDropped))?
    }
}

#[derive(Debug)]
pub struct AckSender {
    message_id: MessageId,
    channel: Option<tokio::sync::oneshot::Sender<WaitAckResult>>,
}

impl AckSender {
    pub fn new(
        message_id: MessageId,
        channel: tokio::sync::oneshot::Sender<WaitAckResult>,
    ) -> Self {
        Self {
            message_id,
            channel: Some(channel),
        }
    }
    pub fn send(&mut self, result: WaitAckResult) {
        if let Some(channel) = self.channel.take() {
            // tracing::info!(id=%self.message_id, ?result, "send ack result");
            let send_result = channel.send(result);
            if send_result.is_err() {
                tracing::info!("ack receiver dropped, message_id: {}", self.message_id);
            }
        }
    }
}

impl Drop for AckSender {
    fn drop(&mut self) {
        if let Some(channel) = self.channel.take() {
            tracing::warn!(
                "ack sender dropped without sending ack, message_id: {}",
                self.message_id
            );
            let _ = channel.send(Err(WaitAckError::exception(
                WaitAckErrorException::MessageDropped,
            )));
        }
    }
}
