use std::{task::Waker, time::Duration};

use crate::{
    endpoint::EndpointAddr,
    protocol::endpoint::{Message, MessageAck, MessageHeader, MessageId, MessageTarget},
};

use super::{wait_ack::WaitAck, Node, NodeId};
#[derive(Debug)]
pub struct HoldMessage {
    pub header: MessageHeader,
    pub wait_ack: Option<WaitAck>,
}

impl Node {
    pub fn hold_message(&self, message: Message, wait_ack: Option<WaitAck>) {
        let hold_message = HoldMessage {
            header: message.header,
            wait_ack,
        };
        {
            let mut wg = self.hold_messages.write().unwrap();
            wg.insert(hold_message.header.message_id, hold_message);
        }
    }
    pub fn ack_to_message(&self, from_ep: EndpointAddr, ack: MessageAck) {
        if self.id() == ack.holder {
            let rg = self.hold_messages.read().unwrap();
            if let Some(hm) = rg.get(&ack.ack_to) {
                if let Some(wait) = &hm.wait_ack {
                    wait.status.write().unwrap().insert(from_ep, ack.kind);
                }
            }
        } else {
            // ack to remote
            todo!("ack to remote node");
        }
    }
}
