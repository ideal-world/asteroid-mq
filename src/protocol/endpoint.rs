mod event;
mod message;

use super::{
    node::{raft::proposal::Proposal, Node, NodeRef},
    topic::{Topic, TopicCode, TopicRef},
};
pub use event::*;
pub use message::*;
use serde::{Deserialize, Serialize};
use std::{
    ops::Deref,
    sync::{Arc, Weak},
};
use typeshare::typeshare;

use crate::protocol::interest::Interest;
#[derive(Clone, Debug)]
pub struct LocalEndpoint {
    pub(crate) inner: Arc<LocalEndpointInner>,
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
    pub(crate) attached_topic: TopicRef,
    pub(crate) topic_code: TopicCode,
    pub(crate) interest: Vec<Interest>,
    pub(crate) address: EndpointAddr,
    pub(crate) mail_box: flume::Receiver<Message>,
    pub(crate) mail_addr: flume::Sender<Message>,
}

impl Drop for LocalEndpointInner {
    fn drop(&mut self) {
        let endpoint = self.address;
        if let Some(topic) = self.attached_topic.upgrade() {
            tokio::spawn(async move {
                let node = topic.node();
                let result = node
                    .proposal(Proposal::EpOffline(EndpointOffline {
                        topic_code: topic.code().clone(),
                        endpoint,
                        host: node.id(),
                    }))
                    .await;
                if let Err(err) = result {
                    tracing::error!(?err, "offline endpoint failed");
                }
            });
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
    #[inline]
    pub fn topic(&self) -> Option<Topic> {
        self.attached_topic.upgrade()
    }
    pub fn reference(&self) -> LocalEndpointRef {
        LocalEndpointRef {
            inner: Arc::downgrade(&self.inner),
        }
    }

    pub async fn ack_processed(&self, header: &MessageHeader) -> Result<(), crate::Error> {
        if let Some(topic) = self.topic() {
            topic
                .single_ack(header.ack_processed(self.topic_code.clone(), self.address))
                .await
        } else {
            Err(crate::Error::new(
                "topic not found",
                crate::error::ErrorKind::Offline,
            ))
        }
    }
    pub async fn ack_received(&self, header: &MessageHeader) -> Result<(), crate::Error> {
        if let Some(topic) = self.topic() {
            topic
                .single_ack(header.ack_received(self.topic_code.clone(), self.address))
                .await
        } else {
            Err(crate::Error::new(
                "topic not found",
                crate::error::ErrorKind::Offline,
            ))
        }
    }
    pub async fn ack_failed(&self, header: &MessageHeader) -> Result<(), crate::Error> {
        if let Some(topic) = self.topic() {
            topic
                .single_ack(header.ack_failed(self.topic_code.clone(), self.address))
                .await
        } else {
            Err(crate::Error::new(
                "topic not found",
                crate::error::ErrorKind::Offline,
            ))
        }
    }
    pub(crate) fn push_message(&self, message: Message) {
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
    pub async fn update_interest(&self, interests: Vec<Interest>) -> Result<(), crate::Error> {
        if let Some(topic) = self.topic() {
            let node = topic.node();
            node.proposal(Proposal::EpInterest(EndpointInterest {
                topic_code: topic.code().clone(),
                endpoint: self.address,
                interests,
            }))
            .await
        } else {
            Err(crate::Error::new(
                "topic not found",
                crate::error::ErrorKind::Offline,
            ))
        }
    }
}

impl MessageHeader {
    #[inline(always)]
    pub fn ack(
        &self,
        topic_code: TopicCode,
        from: EndpointAddr,
        kind: MessageStatusKind,
    ) -> MessageAck {
        MessageAck {
            ack_to: self.message_id,
            kind,
            from,
            topic_code,
        }
    }
    #[inline(always)]
    pub fn ack_received(&self, topic_code: TopicCode, from: EndpointAddr) -> MessageAck {
        self.ack(topic_code, from, MessageStatusKind::Received)
    }
    #[inline(always)]
    pub fn ack_processed(&self, topic_code: TopicCode, from: EndpointAddr) -> MessageAck {
        self.ack(topic_code, from, MessageStatusKind::Processed)
    }
    #[inline(always)]
    pub fn ack_failed(&self, topic_code: TopicCode, from: EndpointAddr) -> MessageAck {
        self.ack(topic_code, from, MessageStatusKind::Failed)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
#[typeshare(serialized_as = "String")]
pub struct EndpointAddr {
    pub bytes: [u8; 16],
}

impl Serialize for EndpointAddr {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            use base64::Engine;
            serializer.serialize_str(&base64::engine::general_purpose::URL_SAFE.encode(self.bytes))
        } else {
            <[u8; 16]>::serialize(&self.bytes, serializer)
        }
    }
}

impl<'de> Deserialize<'de> for EndpointAddr {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            use base64::Engine;
            use serde::de::Error;
            let s = String::deserialize(deserializer)?;
            let bytes = base64::engine::general_purpose::URL_SAFE
                .decode(s.as_bytes())
                .map_err(D::Error::custom)?;
            if bytes.len() != 16 {
                return Err(D::Error::custom("invalid length"));
            }
            let mut addr = [0; 16];
            addr.copy_from_slice(&bytes);
            Ok(Self { bytes: addr })
        } else {
            let bytes = <[u8; 16]>::deserialize(deserializer)?;
            Ok(Self {
                bytes
            })
        }
    }
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
    pub fn hash64(&self) -> u64 {
        use std::hash::{DefaultHasher, Hasher};
        let mut hasher = DefaultHasher::new();
        Hasher::write(&mut hasher, &self.bytes);
        Hasher::finish(&hasher)
    }
}

impl Node {}
