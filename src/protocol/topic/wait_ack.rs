use std::{
    collections::{HashMap, HashSet},
    future::Future,
    sync::RwLock,
    task::Poll,
    time::Instant,
};

use crate::protocol::endpoint::{
    EndpointAddr, Message, MessageAckExpectKind, MessageId, MessageStatusKind,
};

#[derive(Debug)]
pub struct WaitAck {
    pub expect: MessageAckExpectKind,
    pub status: RwLock<HashMap<EndpointAddr, MessageStatusKind>>,
    pub timeout: Option<Instant>,
}

#[derive(Debug)]
pub struct WaitAckError {
    pub status: HashMap<EndpointAddr, MessageStatusKind>,
    pub exception: Option<WaitAckErrorException>,
}
#[derive(Debug)]
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
#[derive(Debug)]
pub enum WaitAckErrorException {
    MessageDropped,
    Overflow,
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
        Self {
            status: status.into(),
            timeout: None,
            expect,
        }
    }
}

pin_project_lite::pin_project! {
    pub struct WaitAckHandle {
        pub(crate) message_id: MessageId,
        #[pin]
        pub(crate) result: flume::r#async::RecvFut<'static, Result<WaitAckSuccess, WaitAckError>>,
    }

}

impl WaitAckHandle {
    pub fn message_id(&self) -> MessageId {
        self.message_id
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
