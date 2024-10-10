use std::{
    collections::{HashMap, HashSet},
    future::Future,
    task::Poll,
};

pub use asteroid_mq_model::{WaitAckError, WaitAckErrorException, WaitAckResult, WaitAckSuccess};
use serde::{Deserialize, Serialize};

use crate::protocol::{endpoint::EndpointAddr, message::*};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WaitAck {
    pub expect: MessageAckExpectKind,
    pub status: HashMap<EndpointAddr, MessageStatusKind>,
}

impl WaitAck {
    pub fn new(expect: MessageAckExpectKind, ep_list: HashSet<EndpointAddr>) -> Self {
        let status = HashMap::<EndpointAddr, MessageStatusKind>::from_iter(
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
    pub fn new(id: MessageId) -> (tokio::sync::oneshot::Sender<WaitAckResult>, WaitAckHandle) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        (
            tx,
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
