use super::{
    message::*,
    node::{
        raft::proposal::{EndpointInterest, EndpointOffline, Proposal},
        Node, NodeRef,
    },
    topic::{Topic, TopicCode, TopicRef},
};
use std::{
    ops::Deref,
    sync::{Arc, Weak},
};

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
                    .propose(Proposal::EpOffline(EndpointOffline {
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

    /// Send a processed acknowledgement
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

    /// Send a received acknowledgement
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

    /// Send a failed acknowledgement
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
    pub async fn next_message(&self) -> Option<Message> {
        self.mail_box.recv_async().await.ok()
    }

    /// Update the interest of this endpoint
    pub async fn update_interest(&self, interests: Vec<Interest>) -> Result<(), crate::Error> {
        if let Some(topic) = self.topic() {
            let node = topic.node();
            node.propose(Proposal::EpInterest(EndpointInterest {
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

pub use asteroid_mq_model::EndpointAddr;

impl Node {}
