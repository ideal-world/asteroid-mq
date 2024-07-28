use std::sync::Arc;

use crate::protocol::{
    ee::{Message, MessageAck},
    nn::Node,
};
#[derive(Clone, Debug)]
pub struct Endpoint {
    pub attached_node: std::sync::Weak<Node>,
    pub address: EndpointAddr,
}

impl Endpoint {
    pub async fn send_message(&self, message: Message) {
        todo!("send message")
    }
    pub async fn ack(&self, ack: MessageAck) {
        todo!("ack message")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EndpointAddr {
    pub bytes: [u8; 16],
}

impl Node {
    pub fn create_endpoint(self: Arc<Self>, addr: EndpointAddr) -> Arc<Endpoint> {
        let ep = Arc::new(Endpoint {
            attached_node: Arc::downgrade(&self),
            address: addr,
        });
        self.endpoints.write().unwrap().insert(addr, ep.clone());
        ep
    }
}
