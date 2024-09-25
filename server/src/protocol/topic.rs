//! TOPIC 主题
//!
//!
//!

pub mod durable_message;

use std::{
    borrow::Borrow,
    collections::HashMap,
    hash::Hash,
    ops::Deref,
    sync::{Arc, Weak},
};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use typeshare::typeshare;

use crate::protocol::endpoint::LocalEndpointInner;

use super::{
    endpoint::{EndpointAddr, LocalEndpoint, LocalEndpointRef},
    interest::Interest,
    message::*,
    node::{
        raft::{
            proposal::*,
            state_machine::topic::wait_ack::{WaitAckHandle, WaitAckResult},
        },
        Node,
    },
};
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[typeshare(serialized_as = "String")]
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

impl From<&'_ str> for TopicCode {
    fn from(val: &'_ str) -> Self {
        TopicCode::new(val)
    }
}

impl From<String> for TopicCode {
    fn from(val: String) -> Self {
        TopicCode::new(val)
    }
}

impl From<&'_ [u8] > for TopicCode {
    fn from(val: &'_ [u8]) -> Self {
        TopicCode(Bytes::copy_from_slice(val))
    }
}

impl From<Vec<u8>> for TopicCode {
    fn from(val: Vec<u8>) -> Self {
        TopicCode(Bytes::from(val))
    }
}

impl Serialize for TopicCode {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let string = unsafe { std::str::from_utf8_unchecked(self.0.as_ref()) };
        serializer.serialize_str(string)
    }
}

impl<'de> Deserialize<'de> for TopicCode {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let string = String::deserialize(deserializer)?;
        Ok(Self(Bytes::from(string)))
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
    pub(crate) local_endpoints: Arc<std::sync::RwLock<HashMap<EndpointAddr, LocalEndpointRef>>>,
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
}
impl Topic {
    pub async fn send_message(&self, message: Message) -> Result<WaitAckHandle, crate::Error> {
        let handle = self.wait_ack(message.id()).await;
        self.node()
            .propose(Proposal::DelegateMessage(DelegateMessage {
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
            .propose(Proposal::EpOnline(EndpointOnline {
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
        node.propose(Proposal::EpOffline(ep_offline)).await?;
        Ok(())
    }

    pub(crate) async fn dispatch_message(
        &self,
        message: Message,
        ep: &EndpointAddr,
    ) -> Option<MessageStatusKind> {
        // message is local or edge?
        if let Some(local) = self.get_local_ep(ep) {
            local.upgrade()?.push_message(message);
            Some(MessageStatusKind::Sent)
        } else {
            // message is edge
            let node = self.node();
            tracing::debug!(?ep, "dispatch message to edge");
            let node_id = node.get_edge_routing(ep)?;
            tracing::debug!(?node_id, "dispatch message to edge");
            let edge = node.get_edge_connection(node_id)?;
            let result = edge.push_message(ep, message);
            if let Err(err) = result {
                tracing::warn!(?err, "push message failed");
                return Some(MessageStatusKind::Unreachable);
            }
            Some(MessageStatusKind::Sent)
        }
    }
    pub(crate) async fn single_ack(&self, ack: MessageAck) -> Result<(), crate::Error> {
        self.node()
            .propose(Proposal::SetState(SetState {
                topic: self.code().clone(),
                update: MessageStateUpdate::new(ack.ack_to, HashMap::from([(ack.from, ack.kind)])),
            }))
            .await
    }
}
