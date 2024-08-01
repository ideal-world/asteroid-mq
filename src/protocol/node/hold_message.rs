use std::task::Poll;

use crate::protocol::{
    endpoint::{EndpointAddr, Message, MessageAck, MessageAckKind, MessageHeader},
    node::event::N2nPacket,
};

use super::{
    wait_ack::{WaitAck, WaitAckError},
    Node,
};
#[derive(Debug)]
pub struct HoldMessage {
    pub header: MessageHeader,
    pub wait_ack: Option<WaitAck>,
}

impl Node {
    pub(crate) fn hold_message(&self, message: Message, wait_ack: Option<WaitAck>) {
        let hold_message = HoldMessage {
            header: message.header,
            wait_ack,
        };
        {
            let mut wg = self.hold_messages.write().unwrap();
            wg.insert(hold_message.header.message_id, hold_message);
        }
    }
    pub(crate) fn poll_ack(&self, wait_ack: &WaitAck) -> Poll<Result<(), WaitAckError>> {
        let rg = wait_ack.status.read().unwrap();
        tracing::debug!(?rg, ep_list = ?wait_ack.ep_list, "poll_ack");
        if rg.len() == wait_ack.ep_list.len() {
            for (ep, ack) in rg.iter() {
                if !ack.is_resolved(wait_ack.expect) {
                    return Poll::Pending;
                }
            }
            // resolved
            let status: Vec<(EndpointAddr, MessageAckKind)> =
                rg.iter().map(|(ep, ack)| (*ep, *ack)).collect();
            let mut fail_list: Vec<EndpointAddr> = Vec::new();
            let mut unreachable_list: Vec<EndpointAddr> = Vec::new();
            for (ep, ack) in status {
                match ack {
                    MessageAckKind::Failed => {
                        fail_list.push(ep);
                    }
                    MessageAckKind::Unreachable => {
                        unreachable_list.push(ep);
                    }
                    _ => {}
                }
            }
            if fail_list.is_empty() && unreachable_list.is_empty() {
                Poll::Ready(Ok(()))
            } else {
                Poll::Ready(Err(WaitAckError {
                    ep_list: wait_ack.ep_list.iter().cloned().collect(),
                    failed_list: fail_list,
                    unreachable_list,
                    timeout_list: Vec::new(),
                    exception: None,
                }))
            }
        } else {
            Poll::Pending
        }
    }
    pub(crate) fn local_ack(&self, ack: MessageAck) {
        let rg = self.hold_messages.read().unwrap();
        if let Some(hm) = rg.get(&ack.ack_to) {
            if let Some(wait_ack) = &hm.wait_ack {
                {
                    let mut wg = wait_ack.status.write().unwrap();
                    wg.insert(ack.from, ack.kind);
                }
                match self.poll_ack(wait_ack) {
                    Poll::Ready(result) => {
                        tracing::debug!(?ack, ?result, "ack resolved");
                        drop(rg);
                        if let Some(hm) = self.hold_messages.write().unwrap().remove(&ack.ack_to) {
                            if let Some(wait_ack) = hm.wait_ack {
                                // we don't need to check if the receiver is still alive
                                let _ = wait_ack.reporter.send(result);
                                tracing::debug!("sendout");
                            }
                        }
                    }
                    Poll::Pending => {}
                }
            } else {
                // don't expect ack
            }
        } else {
            // message not found
        }
    }
    pub fn ack_to_message(&self, ack: MessageAck) {
        if self.is(ack.holder) {
            self.local_ack(ack);
        } else if let Some(next_jump) = self.get_next_jump(ack.holder) {
            if let Err(e) =
                self.send_packet(N2nPacket::event(self.new_ack(ack.holder, ack)), next_jump)
            {
            }
        } else {
            // handle unreachable
        }
    }
}
