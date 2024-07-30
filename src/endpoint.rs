use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{self, Arc, Mutex},
};

use bytes::Bytes;

use crate::{
    interest::{Interest, Subject},
    protocol::{
        endpoint::{Message, MessageAck, MessageAckKind, MessageId},
        node::{
            event::{N2NEventPacket, N2NMessageEvent, NodeTrace},
            wait_ack::WaitAck,
            Node, NodeId, NodeInner, NodeRef,
        },
    },
};
#[derive(Clone, Debug)]
pub struct LocalEndpoint {
    pub attached_node: NodeRef,
    pub interest: Vec<Interest>,
    pub address: EndpointAddr,
    pub mail_box: flume::Receiver<Message>,
    pub mail_addr: flume::Sender<Message>,
}

impl LocalEndpoint {
    #[inline]
    pub fn node(&self) -> Option<Node> {
        self.attached_node.upgrade()
    }
    pub async fn send_message(&self, message: Message) {
        if let Some(node) = self.node() {
            if node.is_edge() {
                todo!("send to edge")
            } else {
                node.create_cluster_e2e_message_task(message).await;
            }
        }
    }
    pub fn send_ack(&self, ack: MessageAck) {
        if let Some(attached_node) = self.node() {
            attached_node.ack_to_message(self.address, ack)
        }
    }
    pub fn push_message(&self, message: Message) {
        self.mail_addr
            .send(message)
            .expect("ep self hold the receiver");
    }
    pub async fn next_message(&self) -> Message {
        self.mail_box
            .recv_async()
            .await
            .expect("ep self hold a sender")
    }
}

impl Message {
    #[inline(always)]
    pub fn ack(&self, kind: MessageAckKind) -> MessageAck {
        MessageAck {
            ack_to: self.id(),
            holder: self.header.holder_node,
            kind,
        }
    }
    #[inline(always)]
    pub fn ack_received(&self) -> MessageAck {
        self.ack(MessageAckKind::Received)
    }
    #[inline(always)]
    pub fn ack_processed(&self) -> MessageAck {
        self.ack(MessageAckKind::Processed)
    }
    #[inline(always)]
    pub fn ack_failed(&self) -> MessageAck {
        self.ack(MessageAckKind::Failed)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EndpointAddr {
    pub bytes: [u8; 16],
}

impl EndpointAddr {
    pub fn random() -> Self {
        thread_local! {
            static COUNTER: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
        }
        let timestamp = crate::util::timestamp_sec();
        let counter = COUNTER.with(|c| {
            let v = c.get();
            c.set(v.wrapping_add(1));
            v
        });
        let eid = crate::util::executor_digest() as u32;
        let mut bytes = [0; 16];
        bytes[0..8].copy_from_slice(&timestamp.to_be_bytes());
        bytes[8..12].copy_from_slice(&counter.to_be_bytes());
        bytes[12..16].copy_from_slice(&eid.to_be_bytes());
        Self { bytes }
    }
}

impl Node {
    pub fn create_endpoint(
        &self,
        interests: impl IntoIterator<Item = Interest>,
    ) -> Arc<LocalEndpoint> {
        let channel = flume::unbounded();
        let ep = Arc::new(LocalEndpoint {
            attached_node: self.node_ref(),
            address: EndpointAddr::random(),
            mail_box: channel.1,
            mail_addr: channel.0,
            interest: interests.into_iter().collect(),
        });
        self.local_endpoints
            .write()
            .unwrap()
            .insert(ep.address, ep.clone());
        {
            let mut wg = self.ep_interest_map.write().unwrap();
            for interest in &ep.interest {
                tracing::debug!(map = ?&*wg);
                wg.insert(interest.clone(), ep.address);
            }
            if self.is_edge() {
                todo!("notify remote cluster node!")
            }
        }
        ep
    }
    pub fn collect_addr_by_subjects<'i>(
        &self,
        subjects: impl Iterator<Item = &'i Subject>,
    ) -> HashSet<EndpointAddr> {
        let mut ep_collect = HashSet::new();
        let rg = self.ep_interest_map.read().unwrap();
        tracing::debug!(?rg);
        for subject in subjects {
            ep_collect.extend(rg.find(subject));
        }
        ep_collect
    }
    pub fn get_local_ep(&self, ep: &EndpointAddr) -> Option<Arc<LocalEndpoint>> {
        self.local_endpoints.read().unwrap().get(ep).cloned()
    }
    pub fn get_remote_ep(&self, ep: &EndpointAddr) -> Option<NodeId> {
        self.ep_routing_table.read().unwrap().get(ep).copied()
    }
    pub async fn create_cluster_e2e_message_task(&self, message: Message) -> Result<(), ()> {
        match &message.header.target {
            crate::protocol::endpoint::MessageTarget::Durable(_) => todo!(),
            crate::protocol::endpoint::MessageTarget::Online(_) => todo!(),
            crate::protocol::endpoint::MessageTarget::Available(_) => todo!(),
            crate::protocol::endpoint::MessageTarget::Push(target) => {
                let ep_collect = self.collect_addr_by_subjects(target.subjects.iter());
                tracing::trace!(?ep_collect);
                for ep in &ep_collect {
                    // prefer local endpoint
                    if let Some(ep) = self.get_local_ep(ep) {
                        ep.push_message(message.clone());
                        let wait_ack = message
                            .ack_kind()
                            .map(|ack_kind| WaitAck::new(ack_kind, ep_collect));
                        self.hold_message(message, wait_ack);
                        return Ok(());
                    }
                }
                for ep in &ep_collect {
                    if let Some(remote_node) = self.get_remote_ep(ep) {
                        if let Some(next_jump) = self.get_next_jump(remote_node) {
                            let message_event = self.new_n2n_message(remote_node, Bytes::new());
                            match self.send_packet(N2NEventPacket::message(message_event), next_jump) {
                                Ok(_) => {
                                    
                                },
                                Err(_) => todo!(),
                            }
                        }
                    }
                }
                return Err(());
            }
        }
        todo!()
    }
}

pub struct AckWaitList {
    pub message_id: MessageId,
    pub expect_stage: Option<MessageAckKind>,
    pub eps: Vec<EndpointAddr>,
    pub attached_node: NodeRef,
    pub wait_map: HashMap<EndpointAddr, Option<MessageAckKind>>,
}

impl std::future::Future for AckWaitList {
    type Output = Result<(), ()>;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let Some(node) = self.attached_node.upgrade() else {
            return std::task::Poll::Ready(Err(()));
        };
        let this = self.get_mut();
        if this.expect_stage.is_none() {
            return std::task::Poll::Ready(Ok(()));
        }
        for (addr, state) in &this.wait_map {
            todo!("check ack state");
        }
        std::task::Poll::Pending
    }
}
