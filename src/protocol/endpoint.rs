use tracing::instrument;
mod event;
mod message;

use super::{
    codec::CodecType,
    node::{
        event::{N2nEvent, N2nEventKind},
        Node, NodeId, NodeRef,
    },
};
pub use event::*;
pub use message::*;
use std::{
    collections::{HashMap, HashSet},
    ops::Deref,
    sync::{Arc, Weak},
};

use crate::{
    protocol::interest::{Interest, Subject},
    protocol::node::{
        event::N2nPacket,
        wait_ack::{WaitAck, WaitAckHandle},
    },
    TimestampSec,
};
#[derive(Clone, Debug)]
pub struct LocalEndpoint {
    inner: Arc<LocalEndpointInner>,
}

impl Deref for LocalEndpoint {
    type Target = LocalEndpointInner;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Clone, Debug)]
pub struct LocalEndpointInner {
    pub(crate) attached_node: NodeRef,
    pub(crate) interest: Vec<Interest>,
    pub(crate) address: EndpointAddr,
    pub(crate) mail_box: flume::Receiver<Message>,
    pub(crate) mail_addr: flume::Sender<Message>,
}

impl Drop for LocalEndpointInner {
    fn drop(&mut self) {
        if let Some(node) = self.attached_node.upgrade() {
            node.delete_endpoint(self.address);
        }
    }
}

#[derive(Clone, Debug)]
pub struct LocalEndpointRef {
    pub inner: Weak<LocalEndpointInner>,
}

impl LocalEndpointRef {
    pub fn upgrade(&self) -> Option<LocalEndpoint> {
        self.inner.upgrade().map(|inner| LocalEndpoint { inner })
    }
}

impl LocalEndpoint {
    #[inline]
    pub fn node(&self) -> Option<Node> {
        self.attached_node.upgrade()
    }
    pub fn reference(&self) -> LocalEndpointRef {
        LocalEndpointRef {
            inner: Arc::downgrade(&self.inner),
        }
    }
    pub async fn send_message(&self, message: Message) -> Result<WaitAckHandle, ()> {
        if let Some(node) = self.node() {
            if node.is_edge() {
                todo!("send to edge")
            } else {
                node.hold_new_message(message).await
            }
        } else {
            Err(())
        }
    }
    pub fn ack_processed(&self, message: &Message) {
        if let Some(attached_node) = self.node() {
            attached_node.ack_to_message(message.ack_processed(self.address))
        }
    }
    pub fn ack_received(&self, message: &Message) {
        if let Some(attached_node) = self.node() {
            attached_node.ack_to_message(message.ack_received(self.address))
        }
    }
    pub fn ack_failed(&self, message: &Message) {
        if let Some(attached_node) = self.node() {
            attached_node.ack_to_message(message.ack_failed(self.address))
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
    pub fn ack(&self, from: EndpointAddr, kind: MessageAckKind) -> MessageAck {
        MessageAck {
            ack_to: self.id(),
            holder: self.header.holder_node,
            kind,
            from,
        }
    }
    #[inline(always)]
    pub fn ack_received(&self, from: EndpointAddr) -> MessageAck {
        self.ack(from, MessageAckKind::Received)
    }
    #[inline(always)]
    pub fn ack_processed(&self, from: EndpointAddr) -> MessageAck {
        self.ack(from, MessageAckKind::Processed)
    }
    #[inline(always)]
    pub fn ack_failed(&self, from: EndpointAddr) -> MessageAck {
        self.ack(from, MessageAckKind::Failed)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct EndpointAddr {
    pub bytes: [u8; 16],
}

impl std::fmt::Debug for EndpointAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("EndpointAddr")
            .field(&crate::util::dashed(&[
                crate::util::hex(&self.bytes[0..8]),
                crate::util::hex(&self.bytes[8..12]),
                crate::util::hex(&self.bytes[12..16]),
            ]))
            .finish()
    }
}

impl EndpointAddr {
    pub fn new_snowflake() -> Self {
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
    pub fn create_endpoint(&self, interests: impl IntoIterator<Item = Interest>) -> LocalEndpoint {
        let channel = flume::unbounded();
        let ep = LocalEndpoint {
            inner: Arc::new(LocalEndpointInner {
                attached_node: self.node_ref(),
                address: EndpointAddr::new_snowflake(),
                mail_box: channel.1,
                mail_addr: channel.0,
                interest: interests.into_iter().collect(),
            }),
        };
        self.local_endpoints
            .write()
            .unwrap()
            .insert(ep.address, ep.reference());
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
        {
            let mut wg = self.ep_routing_table.write().unwrap();
            wg.insert(ep.address, self.id());
        }
        {
            let mut wg = self.ep_latest_active.write().unwrap();
            wg.insert(ep.address, TimestampSec::now());
        }
        let payload = EndpointOnline {
            endpoint: ep.address,
            interests: ep.interest.clone(),
            host: self.id(),
        }
        .encode_to_bytes();
        let peers = self.known_peer_cluster();
        tracing::debug!("notify peers: {peers:?}");
        for node in peers {
            let packet = N2nPacket::event(N2nEvent {
                to: node,
                trace: self.new_trace(),
                kind: N2nEventKind::EpOnline,
                payload: payload.clone(),
            });
            self.send_packet(packet, node);
        }
        ep
    }
    pub fn delete_endpoint(&self, addr: EndpointAddr) {
        {
            let mut local_endpoints = self.local_endpoints.write().unwrap();
            let mut ep_interest_map = self.ep_interest_map.write().unwrap();
            let mut ep_routing_table = self.ep_routing_table.write().unwrap();
            let mut ep_latest_active = self.ep_latest_active.write().unwrap();
            ep_interest_map.delete(&addr);
            ep_routing_table.remove(&addr);
            ep_latest_active.remove(&addr);
            local_endpoints.remove(&addr);
        }
        let ep_offline = EndpointOffline {
            endpoint: addr,
            host: self.id(),
        };
        let payload = ep_offline.encode_to_bytes();
        for peer in self.known_peer_cluster() {
            let packet = N2nPacket::event(N2nEvent {
                to: peer,
                trace: self.new_trace(),
                kind: N2nEventKind::EpOffline,
                payload: payload.clone(),
            });
            self.send_packet(packet, peer);
        }
    }
    pub fn collect_addr_by_subjects<'i>(
        &self,
        subjects: impl Iterator<Item = &'i Subject>,
    ) -> HashSet<EndpointAddr> {
        let mut ep_collect = HashSet::new();
        let rg = self.ep_interest_map.read().unwrap();
        for subject in subjects {
            ep_collect.extend(rg.find(subject));
        }
        ep_collect
    }
    pub fn get_local_ep(&self, ep: &EndpointAddr) -> Option<LocalEndpointRef> {
        self.local_endpoints.read().unwrap().get(ep).cloned()
    }
    pub fn push_message_to_local_ep(
        &self,
        ep: &EndpointAddr,
        message: Message,
    ) -> Result<(), Message> {
        if let Some(ep) = self.get_local_ep(ep) {
            if let Some(sender) = ep.upgrade() {
                sender.push_message(message);
                return Ok(());
            }
        }
        Err(message)
    }
    pub fn get_remote_ep(&self, ep: &EndpointAddr) -> Option<NodeId> {
        self.ep_routing_table.read().unwrap().get(ep).copied()
    }
    pub fn resolve_node_ep_map(
        &self,
        ep_list: impl Iterator<Item = EndpointAddr>,
    ) -> HashMap<NodeId, Vec<EndpointAddr>> {
        let rg_local = self.local_endpoints.read().unwrap();
        let rg_routing = self.ep_routing_table.read().unwrap();
        let mut resolve_map = <HashMap<NodeId, Vec<EndpointAddr>>>::new();
        for ep in ep_list {
            if rg_local.get(&ep).is_some() {
                resolve_map.entry(self.id()).or_default().push(ep);
            } else if let Some(node) = rg_routing.get(&ep) {
                resolve_map.entry(*node).or_default().push(ep);
            }
        }
        resolve_map
    }
    #[instrument(skip(self, message), fields(node_id=?self.id()))]
    pub async fn hold_new_message(&self, message: Message) -> Result<WaitAckHandle, ()> {
        let ep_collect = self.collect_addr_by_subjects(message.header.subjects.iter());
        let (result_report, result_recv) = flume::bounded(1);
        tracing::debug!(?ep_collect, "hold new message");
        match &message.header.target_kind {
            crate::protocol::endpoint::MessageTargetKind::Durable => todo!(),
            crate::protocol::endpoint::MessageTargetKind::Online => {
                if let Some(ack_kind) = message.ack_kind() {
                    let wait_ack = WaitAck::new(ack_kind, ep_collect.clone(), result_report);
                    self.hold_message(message.clone(), Some(wait_ack));
                }
                let map = self.resolve_node_ep_map(ep_collect.into_iter());
                tracing::debug!(?map, "resolve node ep map");
                for (node, eps) in map {
                    if self.is(node) {
                        for ep in &eps {
                            self.push_message_to_local_ep(ep, message.clone());
                        }
                    } else if let Some(next_jump) = self.get_next_jump(node) {
                        tracing::trace!(node_id=?node, eps=?eps, "send to remote node");
                        let message_event = self.new_cast_message(
                            node,
                            CastMessage {
                                target_eps: eps,
                                message: message.clone(),
                            },
                        );
                        match self.send_packet(N2nPacket::event(message_event), next_jump) {
                            Ok(_) => {}
                            Err(_) => todo!(),
                        }
                    } else {
                        todo!("unresolved remote node")
                    }
                }
                return Ok(message.create_wait_handle(result_recv));
            }
            crate::protocol::endpoint::MessageTargetKind::Available => todo!(),
            crate::protocol::endpoint::MessageTargetKind::Push => {
                for ep in &ep_collect {
                    // prefer local endpoint
                    if self.push_message_to_local_ep(ep, message.clone()).is_ok() {
                        let wait_ack = message
                            .ack_kind()
                            .map(|ack_kind| WaitAck::new(ack_kind, ep_collect, result_report));
                        self.hold_message(message.clone(), wait_ack);
                        return Ok(message.create_wait_handle(result_recv));
                    }
                }
                for ep in &ep_collect {
                    if let Some(remote_node) = self.get_remote_ep(ep) {
                        if let Some(next_jump) = self.get_next_jump(remote_node) {
                            let message_event = self.new_cast_message(
                                remote_node,
                                CastMessage {
                                    target_eps: vec![*ep],
                                    message: message.clone(),
                                },
                            );
                            match self.send_packet(N2nPacket::event(message_event), next_jump) {
                                Ok(_) => {
                                    let wait_ack = message.ack_kind().map(|ack_kind| {
                                        WaitAck::new(ack_kind, ep_collect, result_report)
                                    });
                                    self.hold_message(message.clone(), wait_ack);
                                    return Ok(message.create_wait_handle(result_recv));
                                }
                                Err(_) => todo!(),
                            }
                        }
                    }
                }
                return Err(());
            }
        }
    }
}
