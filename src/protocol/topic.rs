//! TOPIC 主题
//!
//!
//!

pub mod config;
pub mod durable_message;
pub mod hold_message;
pub mod wait_ack;

use std::{
    borrow::Borrow,
    collections::{HashMap, HashSet},
    hash::Hash,
    ops::Deref,
    sync::{Arc, RwLock, Weak},
};

use bytes::Bytes;
use config::TopicConfig;
use crossbeam::sync::ShardedLock;
use durable_message::{DurabilityService, DurableMessage, LoadTopic, UnloadTopic};
use hold_message::{HoldMessage, MessagePollContext, MessageQueue};
use tracing::instrument;
use wait_ack::{WaitAck, WaitAckError, WaitAckErrorException, WaitAckHandle};

use crate::{
    impl_codec,
    protocol::{endpoint::LocalEndpointInner, node::raft::LogEntry},
    TimestampSec,
};

use super::{
    codec::CodecType,
    endpoint::{
        EndpointAddr, EndpointOffline, EndpointOnline, EpInfo, LocalEndpoint, LocalEndpointRef,
        Message, MessageId, MessageStateUpdate,
    },
    interest::{Interest, InterestMap, Subject},
    node::{
        event::{N2nEvent, N2nEventKind, N2nPacket},
        Node, NodeId,
    },
};
#[derive(Debug, Clone, PartialEq, Eq, Hash)]

/// code are expect to be a valid utf8 string
pub struct TopicCode(Bytes);
impl TopicCode {
    pub fn new<B: Into<String>>(code: B) -> Self {
        Self(Bytes::from(code.into()))
    }
    pub const fn const_new(code: &'static str) -> Self {
        Self(Bytes::from_static(code.as_bytes()))
    }
}

impl std::fmt::Display for TopicCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        unsafe { f.write_str(std::str::from_utf8_unchecked(&self.0)) }
    }
}
impl CodecType for TopicCode {
    fn decode(bytes: Bytes) -> Result<(Self, Bytes), super::codec::DecodeError> {
        Bytes::decode(bytes).and_then(|(s, bytes)| {
            std::str::from_utf8(&s)
                .map_err(|e| super::codec::DecodeError::new::<TopicCode>(e.to_string()))?;
            Ok((TopicCode(s), bytes))
        })
    }

    fn encode(&self, buf: &mut bytes::BytesMut) {
        self.0.encode(buf)
    }
}

impl Borrow<str> for TopicCode {
    fn borrow(&self) -> &str {
        todo!()
    }
}

#[derive(Debug, Clone)]
pub struct Topic {
    pub node: Node,
    pub(crate) inner: Arc<TopicInner>,
}

impl Deref for Topic {
    type Target = TopicInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl TopicInner {
    pub fn code(&self) -> &TopicCode {
        &self.config.code
    }
    pub(crate) fn get_ep_sync(&self) -> Vec<EpInfo> {
        let ep_interest_map = self.ep_interest_map.read().unwrap();
        let ep_latest_active = self.ep_latest_active.read().unwrap();
        let mut eps = Vec::new();
        for (ep, host) in self.ep_routing_table.read().unwrap().iter() {
            if let Some(latest_active) = ep_latest_active.get(ep) {
                eps.push(EpInfo {
                    addr: *ep,
                    host: *host,
                    interests: ep_interest_map
                        .interest_of(ep)
                        .map(|s| s.iter().cloned().collect())
                        .unwrap_or_default(),
                    latest_active: *latest_active,
                });
            }
        }
        eps
    }
    pub(crate) fn load_ep_sync(&self, infos: Vec<EpInfo>) {
        let mut active_wg = self.ep_latest_active.write().unwrap();
        let mut routing_wg = self.ep_routing_table.write().unwrap();
        let mut interest_wg = self.ep_interest_map.write().unwrap();
        for ep in infos {
            if let Some(existed_record) = active_wg.get(&ep.addr) {
                if *existed_record > ep.latest_active {
                    continue;
                }
            }
            active_wg.insert(ep.addr, ep.latest_active);
            routing_wg.insert(ep.addr, ep.host);
            for interest in &ep.interests {
                interest_wg.insert(interest.clone(), ep.addr);
            }
        }
    }
    pub(crate) fn ep_online(&self, ep_online: EndpointOnline) {
        let mut routing_wg = self.ep_routing_table.write().unwrap();
        let mut interest_wg = self.ep_interest_map.write().unwrap();
        let mut active_wg = self.ep_latest_active.write().unwrap();
        active_wg.insert(ep_online.endpoint, TimestampSec::now());
        routing_wg.insert(ep_online.endpoint, ep_online.host);
        for interest in &ep_online.interests {
            interest_wg.insert(interest.clone(), ep_online.endpoint);
        }
    }
    pub(crate) fn ep_offline(&self, ep: &EndpointAddr) {
        let mut routing_wg = self.ep_routing_table.write().unwrap();
        let mut interest_wg = self.ep_interest_map.write().unwrap();
        let mut active_wg = self.ep_latest_active.write().unwrap();
        active_wg.remove(ep);
        routing_wg.remove(ep);
        interest_wg.delete(ep);
    }

    pub(crate) fn collect_addr_by_subjects<'i>(
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
    pub(crate) fn get_local_ep(&self, ep: &EndpointAddr) -> Option<LocalEndpointRef> {
        self.local_endpoints.read().unwrap().get(ep).cloned()
    }
    pub(crate) fn push_message_to_local_ep(
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
    pub(crate) fn resolve_node_ep_map(
        &self,
        ep_list: impl Iterator<Item = EndpointAddr>,
    ) -> HashMap<NodeId, Vec<EndpointAddr>> {
        let rg_routing = self.ep_routing_table.read().unwrap();
        let mut resolve_map = <HashMap<NodeId, Vec<EndpointAddr>>>::new();
        for ep in ep_list {
            if let Some(node) = rg_routing.get(&ep) {
                resolve_map.entry(*node).or_default().push(ep);
            }
        }
        resolve_map
    }
}
impl Topic {
    pub fn wait_ack(&self, message_id: MessageId) -> WaitAckHandle {
        let queue = self.queue.read().unwrap();
        queue.wait_ack(message_id)
    }
    pub fn reference(&self) -> TopicRef {
        TopicRef {
            node: self.node.clone(),
            inner: Arc::downgrade(&self.inner),
        }
    }

    pub async fn create_endpoint(
        &self,
        interests: impl IntoIterator<Item = Interest>,
    ) -> Result<LocalEndpoint, crate::Error> {
        let channel = flume::unbounded();
        let topic_code = self.code().clone();
        let ep = LocalEndpoint {
            inner: Arc::new(LocalEndpointInner {
                attached_node: self.node.node_ref(),
                address: EndpointAddr::new_snowflake(),
                mail_box: channel.1,
                mail_addr: channel.0,
                interest: interests.into_iter().collect(),
                topic_code: topic_code.clone(),
                attached_topic: self.reference(),
            }),
        };
        let _ = self
            .node
            .commit_log(LogEntry::ep_online(EndpointOnline {
                topic_code: topic_code.clone(),
                endpoint: ep.address,
                interests: ep.interest.clone(),
                host: self.node.id(),
            }))
            .await
            .map_err(crate::Error::contextual("create endpoint"))?;
        self.local_endpoints
            .write()
            .unwrap()
            .insert(ep.address, ep.reference());
        Ok(ep)
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
            host: self.node.id(),
            topic_code: self.code().clone(),
        };
        let payload = ep_offline.encode_to_bytes();
        for peer in self.node.known_peer_cluster() {
            let packet = N2nPacket::event(N2nEvent {
                to: peer,
                trace: self.node.new_trace(),
                kind: N2nEventKind::EpOffline,
                payload: payload.clone(),
            });
            let _ = self.node.send_packet(packet, peer);
        }
    }

    pub(crate) fn dispatch_message(
        &self,
        message: &Message,
        ep_list: impl Iterator<Item = EndpointAddr>,
    ) -> Vec<(EndpointAddr, Result<(), ()>)> {
        let map = self.resolve_node_ep_map(ep_list);
        tracing::debug!(?map, "dispatch message");
        let mut results = vec![];
        for (node, eps) in map {
            if self.node.is(node) {
                for ep in &eps {
                    match self.push_message_to_local_ep(ep, message.clone()) {
                        Ok(_) => {
                            results.push((*ep, Ok(())));
                        }
                        Err(_) => {
                            results.push((*ep, Err(())));
                        }
                    }
                }
            }
        }
        results
    }

    #[instrument(skip(self, message), fields(node_id=?self.node.id(), topic_code=?self.config.code))]
    pub(crate) fn hold_new_message(&self, message: Message) {
        let ep_collect = self.collect_addr_by_subjects(message.header.subjects.iter());
        let hold_message = HoldMessage {
            message: message.clone(),
            wait_ack: WaitAck::new(message.ack_kind(), ep_collect.clone()),
        };
        {
            let mut queue = self.queue.write().unwrap();
            // put in queue
            if let Some(overflow_config) = &self.config.overflow_config {
                let size = u32::from(overflow_config.size) as usize;
                let waiting_size = queue.len();
                if waiting_size >= size {
                    match overflow_config.policy {
                        config::OverflowPolicy::RejectNew => {
                            if let Some(report) =
                                queue.waiting.get_mut().unwrap().remove(&message.id())
                            {
                                let _ = report.send(Err(WaitAckError::exception(
                                    WaitAckErrorException::Overflow,
                                )));
                            }
                        }
                        config::OverflowPolicy::DropOld => {
                            let old = queue.pop().expect("queue at least one element");
                            if let Some(report) =
                                queue.waiting.get_mut().unwrap().remove(&old.message.id())
                            {
                                let _ = report.send(Err(WaitAckError::exception(
                                    WaitAckErrorException::Overflow,
                                )));
                            }
                        }
                    }
                }
            }
            queue.push(hold_message);
        }
        self.update_and_flush(MessageStateUpdate::new_empty(message.id()));
        tracing::debug!(?ep_collect, "hold new message");
    }
}

#[derive(Debug, Default, Clone)]
pub struct TopicRef {
    node: Node,
    inner: Weak<TopicInner>,
}

impl TopicRef {
    pub fn upgrade(&self) -> Option<Topic> {
        self.inner.upgrade().map(|inner| Topic {
            node: self.node.clone(),
            inner,
        })
    }
}

#[derive(Debug)]
pub struct TopicInner {
    pub(crate) config: TopicConfig,
    pub(crate) local_endpoints: ShardedLock<HashMap<EndpointAddr, LocalEndpointRef>>,
    pub(crate) ep_routing_table: ShardedLock<HashMap<EndpointAddr, NodeId>>,
    pub(crate) ep_interest_map: ShardedLock<InterestMap<EndpointAddr>>,
    pub(crate) ep_latest_active: ShardedLock<HashMap<EndpointAddr, TimestampSec>>,
    pub(crate) queue: RwLock<MessageQueue>,
    pub(crate) durability_service: Option<DurabilityService>,
}

pub struct TopicSnapshot {
    pub ep_routing_table: HashMap<EndpointAddr, NodeId>,
    pub ep_interest_map: HashMap<EndpointAddr, HashSet<Interest>>,
    pub ep_latest_active: HashMap<EndpointAddr, TimestampSec>,
    pub queue: Vec<DurableMessage>,
}

impl_codec!(
    struct TopicSnapshot {
        ep_routing_table: HashMap<EndpointAddr, NodeId>,
        ep_interest_map: HashMap<EndpointAddr, HashSet<Interest>>,
        ep_latest_active: HashMap<EndpointAddr, TimestampSec>,
        queue: Vec<DurableMessage>,
    }
);
impl Topic {
    pub fn apply_snapshot(&self, snapshot: TopicSnapshot) {
        {
            let mut ep_routing_table = self.ep_routing_table.write().unwrap();
            let mut ep_interest_map = self.ep_interest_map.write().unwrap();
            let mut ep_latest_active = self.ep_latest_active.write().unwrap();
            let mut queue = self.queue.write().unwrap();
            *ep_routing_table = snapshot.ep_routing_table;
            *ep_interest_map = InterestMap::from_raw(snapshot.ep_interest_map);
            *ep_latest_active = snapshot.ep_latest_active;
            let context = MessagePollContext { topic: self };
            queue.clear();
            for message in snapshot.queue {
                queue.push(HoldMessage::from_durable(message));
            }
            queue.poll_all(&context);
            queue.flush(&context);
        }
    }
}
impl TopicInner {
    pub fn snapshot(&self) -> TopicSnapshot {
        let ep_routing_table = self.ep_routing_table.read().unwrap().clone();
        let ep_interest_map = self.ep_interest_map.read().unwrap().raw.clone();
        let ep_latest_active = self.ep_latest_active.read().unwrap().clone();
        let queue = self
            .queue
            .read()
            .unwrap()
            .hold_messages
            .values()
            .map(|m| m.as_durable())
            .collect();
        TopicSnapshot {
            ep_routing_table,
            ep_interest_map,
            ep_latest_active,
            queue,
        }
    }

    pub fn new<C: Into<TopicConfig>>(config: C) -> Self {
        const DEFAULT_CAPACITY: usize = 128;
        let config: TopicConfig = config.into();
        let capacity = if let Some(ref overflow_config) = config.overflow_config {
            overflow_config.size()
        } else {
            DEFAULT_CAPACITY
        };
        let messages = MessageQueue::new(config.blocking, capacity);
        Self {
            config,
            local_endpoints: Default::default(),
            ep_routing_table: Default::default(),
            ep_interest_map: Default::default(),
            ep_latest_active: Default::default(),
            queue: RwLock::new(messages),
            durability_service: None,
        }
    }
}

impl Node {
    // return: is leader
    pub async fn wait_raft_cluster_ready(&self) -> bool {
        loop {
            {
                let state = self.raft_state_unwrap().read().unwrap();
                if state.role.is_ready() {
                    break state.role.is_leader();
                }
            };
            tokio::task::yield_now().await;
        }
    }
    pub async fn new_topic<C: Into<TopicConfig>>(&self, config: C) -> Result<Topic, crate::Error> {
        self.load_topic(LoadTopic::from_config(config)).await
    }
    pub async fn load_topic(&self, load_topic: LoadTopic) -> Result<Topic, crate::Error> {
        let code = load_topic.config.code.clone();
        let is_leader = self.wait_raft_cluster_ready().await;
        let topic = if is_leader {
            self.commit_log(LogEntry::load_topic(load_topic))
                .await
                .map_err(crate::Error::contextual("new topic"))?;
            self.get_topic(&code).expect("topic should be loaded")
        } else {
            loop {
                if let Some(topic) = self.get_topic(&code) {
                    break topic;
                }
                tokio::task::yield_now().await;
            }
        };
        Ok(topic)
    }
    pub async fn delete_topic(&self, code: TopicCode) {
        let is_leader = self.wait_raft_cluster_ready().await;
        if is_leader {
            self.commit_log(LogEntry::unload_topic(UnloadTopic::new(code)))
                .await
                .expect("cancel topic");
        }
    }
    pub(crate) fn apply_load_topic<C: Into<TopicConfig>>(&self, config: C) -> Topic {
        let topic = Arc::new(TopicInner::new(config));
        self.topics
            .write()
            .unwrap()
            .insert(topic.config.code.clone(), topic.clone());
        self.wrap_topic(topic)
    }
    pub fn remove_topic<Q>(&self, code: &Q)
    where
        TopicCode: Borrow<Q>,
        Q: Hash + Eq,
    {
        if let Some(topic) = self.topics.write().unwrap().remove(code) {
            let mut queue = topic.queue.write().unwrap();
            let waitings = queue.waiting.get_mut().unwrap();
            for (_, report) in waitings.drain() {
                let _ = report.send(Err(WaitAckError::exception(
                    WaitAckErrorException::MessageDropped,
                )));
            }
            queue.clear();
        }
    }
    pub(crate) fn wrap_topic(&self, topic_inner: Arc<TopicInner>) -> Topic {
        Topic {
            node: self.clone(),
            inner: topic_inner.clone(),
        }
    }
    pub fn get_topic<Q>(&self, code: &Q) -> Option<Topic>
    where
        TopicCode: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.topics
            .read()
            .unwrap()
            .get(code)
            .map(|t| self.wrap_topic(t.clone()))
    }
    pub fn get_or_init_topic(&self, code: TopicCode) -> Topic {
        let topic = self.topics.read().unwrap().get(&code).cloned();
        match topic {
            Some(topic) => self.wrap_topic(topic),
            None => {
                let inner = Arc::new(TopicInner::new(code.clone()));
                let topic = Topic {
                    node: self.clone(),
                    inner: inner.clone(),
                };
                self.topics.write().unwrap().insert(code, inner);
                topic
            }
        }
    }
}
