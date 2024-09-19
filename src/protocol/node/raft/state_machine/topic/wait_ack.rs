use std::{
    collections::{HashMap, HashSet},
    future::Future,
    task::Poll,
};

use serde::{Deserialize, Serialize};
use typeshare::typeshare;

use crate::{
    protocol::endpoint::{
        EndpointAddr, Message, MessageAckExpectKind, MessageId, MessageStatusKind,
    },
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WaitAck {
    pub expect: MessageAckExpectKind,
    pub status: HashMap<EndpointAddr, MessageStatusKind>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[typeshare]
pub struct WaitAckError {
    pub status: HashMap<EndpointAddr, MessageStatusKind>,
    pub exception: Option<WaitAckErrorException>,
}



#[derive(Debug, Clone, Serialize, Deserialize)]
#[typeshare]
pub struct WaitAckSuccess {
    pub status: HashMap<EndpointAddr, MessageStatusKind>,
}


impl WaitAckError {
    pub fn exception(exception: WaitAckErrorException) -> Self {
        Self {
            status: HashMap::new(),
            exception: Some(exception),
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[typeshare]
pub enum WaitAckErrorException {
    MessageDropped = 0,
    Overflow = 1,
    NoAvailableTarget = 2,
}

pub enum AckWaitErrorKind {
    Timeout,
    Fail,
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

impl Message {}

impl Future for WaitAckHandle {
    type Output = Result<WaitAckSuccess, WaitAckError>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        this.result
            .poll(cx)
            .map_err(|_| WaitAckError::exception(WaitAckErrorException::MessageDropped))?
    }
}

pub type WaitAckResult = Result<WaitAckSuccess, WaitAckError>;
