//! TOPIC 主题
//!
//!
//!

pub mod durable_message;

use std::{
    borrow::Borrow,
    collections::{HashMap, HashSet},
    hash::Hash,
    ops::Deref,
    sync::{Arc, RwLock, Weak},
    task::Poll,
};

use bytes::Bytes;
use crossbeam::sync::ShardedLock;
use durable_message::{DurabilityService, DurableMessage, LoadTopic, UnloadTopic};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tracing::instrument;

use crate::{impl_codec, protocol::endpoint::LocalEndpointInner, TimestampSec};

use super::{
    codec::CodecType,
    endpoint::{
        DelegateMessage, EndpointAddr, EndpointOffline, EndpointOnline, EpInfo, LocalEndpoint,
        LocalEndpointRef, Message, MessageAck, MessageId, MessageStateUpdate, MessageStatusKind,
        MessageTargetKind, SetState,
    },
    interest::{Interest, InterestMap, Subject},
    node::{
        raft::{
            proposal::Proposal,
            state_machine::topic::{
                config::TopicConfig,
                wait_ack::{WaitAckHandle, WaitAckResult},
            },
            MaybeLoadingRaft,
        },
        Node, NodeId, NodeRef,
    },
};
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]

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

impl Borrow<[u8]> for TopicCode {
    fn borrow(&self) -> &[u8] {
        &self.0
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

#[derive(Debug, Default, Clone)]
pub struct TopicRef {
    inner: Weak<TopicInner>,
}

impl TopicRef {
    pub fn upgrade(&self) -> Option<Topic> {
        self.inner.upgrade().map(|inner| Topic { inner })
    }
}

#[derive(Debug, Clone)]
pub struct TopicInner {
    pub(crate) code: TopicCode,
    pub(crate) node: Node,
    pub(crate) ack_waiting_pool:
        Arc<tokio::sync::RwLock<HashMap<MessageId, oneshot::Sender<WaitAckResult>>>>,
    pub(crate) local_endpoints: Arc<ShardedLock<HashMap<EndpointAddr, LocalEndpointRef>>>,
}

impl TopicInner {
    pub(crate) fn new(node: Node, code: TopicCode) -> Self {
        Self {
            code,
            node,
            ack_waiting_pool: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            local_endpoints: Arc::new(ShardedLock::new(HashMap::new())),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Topic {
    pub(crate) inner: Arc<TopicInner>,
}
impl Topic {}
impl Deref for Topic {
    type Target = TopicInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl TopicInner {
    pub fn code(&self) -> &TopicCode {
        &self.code
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
}
impl Topic {
    pub async fn send_message(&self, message: Message) -> Result<WaitAckHandle, crate::Error> {
        let handle = self.wait_ack(message.id()).await;
        self.node()
            .proposal(Proposal::DelegateMessage(DelegateMessage {
                topic: self.code().clone(),
                message,
            }))
            .await?;
        Ok(handle)
    }
    pub fn node(&self) -> Node {
        self.node.clone()
    }
    pub async fn wait_ack(&self, id: MessageId) -> WaitAckHandle {
        let (sender, handle) = WaitAckHandle::new(id);
        self.ack_waiting_pool.write().await.insert(id, sender);
        handle
    }
    pub fn reference(&self) -> TopicRef {
        TopicRef {
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
        self.node()
            .proposal(Proposal::EpOnline(EndpointOnline {
                topic_code: topic_code.clone(),
                endpoint: ep.address,
                interests: ep.interest.clone(),
                host: self.node.id(),
            }))
            .await?;
        self.local_endpoints
            .write()
            .unwrap()
            .insert(ep.address, ep.reference());
        Ok(ep)
    }
    pub async fn delete_endpoint(&self, addr: EndpointAddr) -> Result<(), crate::Error> {
        let node = self.node();
        self.local_endpoints.write().unwrap().remove(&addr);
        let ep_offline = EndpointOffline {
            endpoint: addr,
            host: self.node.id(),
            topic_code: self.code().clone(),
        };
        node.proposal(Proposal::EpOffline(ep_offline)).await?;
        Ok(())
    }

    pub(crate) async fn dispatch_message(
        &self,
        message: Message,
        ep: &EndpointAddr,
    ) -> Option<MessageStatusKind> {
        self.get_local_ep(ep)?.upgrade()?.push_message(message);
        Some(MessageStatusKind::Sent)
    }
    pub(crate) async fn single_ack(&self, ack: MessageAck) -> Result<(), crate::Error> {
        self.node()
            .proposal(Proposal::SetState(SetState {
                topic: self.code().clone(),
                update: MessageStateUpdate::new(ack.ack_to, HashMap::from([(ack.from, ack.kind)])),
            }))
            .await
    }
    // pub(crate) fn ep_online(&self, endpoint: EndpointAddr, interests: Vec<Interest>, host: NodeId) {
    //     let mut message_need_poll = HashSet::new();
    //     {
    //         let mut routing_wg = self.ep_routing_table;
    //         let mut interest_wg = self.ep_interest_map;
    //         let mut active_wg = self.ep_latest_active;

    //         active_wg.insert(endpoint, TimestampSec::now());
    //         routing_wg.insert(endpoint, host);
    //         for interest in &interests {
    //             interest_wg.insert(interest.clone(), endpoint);
    //         }
    //         let queue = self.queue;
    //         for (id, message) in &queue.hold_messages {
    //             if message.message.header.target_kind == MessageTargetKind::Durable {
    //                 let mut status = message.wait_ack.status;

    //                 if !status.contains_key(&endpoint)
    //                     && message
    //                         .message
    //                         .header
    //                         .subjects
    //                         .iter()
    //                         .any(|s| interest_wg.find(s).contains(&endpoint))
    //                 {
    //                     status.insert(endpoint, MessageStatusKind::Unsent);
    //                     message_need_poll.insert(*id);
    //                 }
    //             }
    //         }
    //     }
    //     for id in message_need_poll {
    //         self.update_and_flush(MessageStateUpdate::new_empty(id));
    //     }
    // }
}
