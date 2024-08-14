use tracing::instrument;
mod event;
mod message;

use super::{
    codec::CodecType,
    node::{
        event::{N2nEvent, N2nEventKind},
        raft::LogEntry,
        Node, NodeId, NodeRef,
    },
    topic::{wait_ack::WaitAckHandle, Topic, TopicCode, TopicRef},
};
pub use event::*;
pub use message::*;
use std::{
    collections::{HashMap, HashSet},
    ops::Deref,
    sync::{Arc, Weak},
};

use crate::{
    protocol::{
        interest::{Interest, Subject},
        node::event::N2nPacket,
        topic::wait_ack::WaitAck,
    },
    TimestampSec,
};
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
        if let Some(topic) = self.attached_topic.upgrade() {
            topic.delete_endpoint(self.address);
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
    pub async fn send_message(&self, message: Message) -> Result<WaitAckHandle, ()> {
        if let Some(topic) = self.topic() {
            if topic.node.is_edge() {
                todo!("send to edge")
            } else {
                let handle = topic.wait_ack(message.id());
                topic
                    .node
                    .commit_log(LogEntry::delegate_message(message))
                    .await;
                Ok(handle)
            }
        } else {
            Err(())
        }
    }
    pub fn ack_processed(&self, message: &Message) {
        if let Some(topic) = self.topic() {
            topic.handle_ack(message.ack_processed(self.topic_code.clone(), self.address))
        }
    }
    pub fn ack_received(&self, message: &Message) {
        if let Some(topic) = self.topic() {
            topic.handle_ack(message.ack_received(self.topic_code.clone(), self.address))
        }
    }
    pub fn ack_failed(&self, message: &Message) {
        if let Some(topic) = self.topic() {
            topic.handle_ack(message.ack_failed(self.topic_code.clone(), self.address))
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
    pub fn ack(
        &self,
        topic_code: TopicCode,
        from: EndpointAddr,
        kind: MessageStatusKind,
    ) -> MessageAck {
        MessageAck {
            ack_to: self.id(),
            holder: self.header.holder_node,
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

impl Node {}
