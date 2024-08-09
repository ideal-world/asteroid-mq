use std::{
    collections::{HashMap, HashSet},
    future::Future,
    sync::RwLock,
    task::Poll,
    time::Instant,
};

use crossbeam::sync::ShardedLock;

use crate::protocol::endpoint::{
    EndpointAddr, Message, MessageAckExpectKind, MessageId, MessageStatusKind,
};

#[derive(Debug)]
pub struct WaitAck {
    pub expect: MessageAckExpectKind,
    pub status: RwLock<HashMap<EndpointAddr, MessageStatusKind>>,
    pub timeout: Option<Instant>,
    pub ep_list: HashSet<EndpointAddr>,
    pub reporter: flume::Sender<Result<WaitAckSuccess, WaitAckError>>,
}

#[derive(Debug)]
pub struct WaitAckError {
    pub ep_list: Vec<EndpointAddr>,
    pub failed_list: Vec<EndpointAddr>,
    pub unreachable_list: Vec<EndpointAddr>,
    pub timeout_list: Vec<EndpointAddr>,
    pub exception: Option<WaitAckErrorException>,
}
#[derive(Debug)]
pub struct WaitAckSuccess {
    pub ep_list: Vec<EndpointAddr>,
}
impl WaitAckError {
    pub fn exception(exception: WaitAckErrorException) -> Self {
        Self {
            ep_list: Vec::new(),
            failed_list: Vec::new(),
            unreachable_list: Vec::new(),
            timeout_list: Vec::new(),
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
    pub fn new(
        expect: MessageAckExpectKind,
        ep_list: HashSet<EndpointAddr>,
        reporter: flume::Sender<Result<WaitAckSuccess, WaitAckError>>,
    ) -> Self {
        let status = HashMap::<EndpointAddr, MessageStatusKind>::from_iter(
            ep_list.iter().map(|ep| (*ep, MessageStatusKind::Unsent)),
        );
        Self {
            status: status.into(),
            timeout: None,
            ep_list,
            expect,
            reporter,
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

impl Message {
    pub(crate) fn create_wait_handle(
        &self,
        recv: flume::Receiver<Result<WaitAckSuccess, WaitAckError>>,
    ) -> WaitAckHandle {
        WaitAckHandle {
            message_id: self.id(),
            result: recv.into_recv_async(),
        }
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
