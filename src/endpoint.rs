use std::{collections::HashSet, sync::Arc};

use crate::{
    interest::Interest,
    protocol::{
        ee::{Message, MessageAck},
        nn::{Node, NodeId},
    },
};
#[derive(Clone, Debug)]
pub struct LocalEndpoint {
    pub attached_node: std::sync::Weak<Node>,
    pub address: EndpointAddr,
}

impl LocalEndpoint {
    #[inline]
    pub fn node(&self) -> Option<Arc<Node>> {
        self.attached_node.upgrade()
    }
    pub async fn send_message(&self, message: Message) {
        if let Some(node) = self.node() {}
        todo!("send message")
    }
    pub async fn ack(&self, ack: MessageAck) {
        todo!("ack message")
    }
    pub fn push_message(&self, message: Message) {
        todo!("push message")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EndpointAddr {
    pub bytes: [u8; 16],
}

impl Node {
    pub fn create_endpoint(self: &Arc<Self>, addr: EndpointAddr) -> Arc<LocalEndpoint> {
        let ep = Arc::new(LocalEndpoint {
            attached_node: Arc::downgrade(self),
            address: addr,
        });
        self.local_endpoints
            .write()
            .unwrap()
            .insert(addr, ep.clone());
        ep
    }
    pub fn collect_addr_by_interests<'i>(
        &self,
        interests: impl Iterator<Item = &'i Interest>,
    ) -> HashSet<EndpointAddr> {
        let mut ep_collect = HashSet::new();
        let rg = self.ep_interest_map.read().unwrap();
        for interest in interests {
            ep_collect.extend(rg.find_all(interest));
        }
        ep_collect
    }
    pub fn send_ep_message(self: &Arc<Self>, message: Message) {
        if self.is_edge() {
            unimplemented!("edge node")
        } else {
            match self.create_cluster_e2e_message_task(message) {
                Ok(ack_wait_list) => {
                },
                Err(_) => todo!(),
            }
            
            todo!()
        }
        // wait ack
    }
    pub fn get_local_ep(&self, ep: &EndpointAddr) -> Option<Arc<LocalEndpoint>> {
        self.local_endpoints.read().unwrap().get(ep).cloned()
    }
    pub fn get_remote_ep(&self, ep: &EndpointAddr) -> Option<NodeId> {
        self.ep_routing_table.read().unwrap().get(ep).copied()
    }
    pub fn create_cluster_e2e_message_task(&self, message: Message) -> Result<AckWaitList, ()> {
        match &message.target {
            crate::protocol::ee::Target::Durable(_) => todo!(),
            crate::protocol::ee::Target::Online(_) => todo!(),
            crate::protocol::ee::Target::Available(_) => todo!(),
            crate::protocol::ee::Target::Push(target) => {
                let ep_collect = self.collect_addr_by_interests(target.interests.iter());
                for ep in &ep_collect {
                    // prefer local endpoint
                    if let Some(ep) = self.get_local_ep(ep) {
                        ep.push_message(message.clone());
                        return Ok(AckWaitList {
                            message,
                            eps: vec![ep.address],
                        });
                    }
                }
                for ep in &ep_collect {
                    if let Some(remote_node) = self.get_remote_ep(ep) {
                        todo!("send to remote");
                        return Ok(AckWaitList {
                            message,
                            eps: vec![*ep],
                        });
                    }
                }
            }
        }
        todo!()
    }
}

pub struct AckWaitList {
    pub message: Message,
    pub eps: Vec<EndpointAddr>,
}

impl AckWaitList {
    
}