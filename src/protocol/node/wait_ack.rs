use std::{
    collections::{HashMap, HashSet},
    task::Poll,
    time::Instant,
};

use crossbeam::sync::ShardedLock;

use crate::protocol::endpoint::{EndpointAddr, MessageAckKind, MessageId};
#[derive(Debug)]
pub struct WaitAck {
    pub expect: MessageAckKind,
    pub status: ShardedLock<HashMap<EndpointAddr, MessageAckKind>>,
    pub timeout: Option<Instant>,
    pub ep_list: HashSet<EndpointAddr>,
}

pub struct WaitAckError {
    ep_list: Vec<EndpointAddr>,
    current_status: HashMap<EndpointAddr, MessageAckKind>,
    kind: AckWaitErrorKind,
}

pub enum AckWaitErrorKind {
    Timeout,
    Fail,
}

impl WaitAck {
    pub fn new(expect: MessageAckKind, ep_list: HashSet<EndpointAddr>) -> Self {
        Self {
            status: Default::default(),
            timeout: None,
            ep_list,
            expect,
        }
    }
    pub fn poll_check(&self) -> Poll<Result<(), WaitAckError>> {
        let rg = self.status.read().unwrap();
        for ep in &self.ep_list {
            match rg.get(ep) {
                Some(ack) if ack.is_reached(self.expect) => {
                    continue;
                }
                Some(ack) if ack.is_failed() => {
                    return Poll::Ready(Err(WaitAckError {
                        ep_list: self.ep_list.iter().cloned().collect(),
                        current_status: rg.clone(),
                        kind: AckWaitErrorKind::Fail,
                    }))
                }
                Some(_) => return Poll::Pending,
                None => return Poll::Pending,
            }
        }
        Poll::Ready(Ok(()))
    }
}
