use std::{
    collections::{HashMap, HashSet},
    future::Future,
    sync::RwLock,
    task::Poll,
    time::Instant,
};

use crate::{impl_codec, protocol::endpoint::{
    EndpointAddr, Message, MessageAckExpectKind, MessageId, MessageStatusKind,
}};

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

impl_codec!(
    struct WaitAckError {
        status: HashMap<EndpointAddr, MessageStatusKind>,
        exception: Option<WaitAckErrorException>,
    }
);

#[derive(Debug)]
pub struct WaitAckSuccess {
    pub status: HashMap<EndpointAddr, MessageStatusKind>,
}

impl_codec!(
    struct WaitAckSuccess {
        status: HashMap<EndpointAddr, MessageStatusKind>,
    }
);


impl WaitAckError {
    pub fn exception(exception: WaitAckErrorException) -> Self {
        Self {
            status: HashMap::new(),
            exception: Some(exception),
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub enum WaitAckErrorException {
    MessageDropped = 0,
    Overflow = 1,
    NoAvailableTarget = 2,
}

impl_codec!(
    enum WaitAckErrorException {
        MessageDropped = 0,
        Overflow = 1,
        NoAvailableTarget = 2,
    }
);

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
