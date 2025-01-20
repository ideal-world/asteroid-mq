use std::{
    collections::HashSet,
    ops::{Deref, DerefMut},
    sync::Weak,
};

use asteroid_mq_model::{EdgeEndpointOnline, EndpointAddr, Interest, Message, TopicCode};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::Instrument;

use crate::{
    node::{ClientNodeError, ClientNodeInner},
    ClientNode,
};

// CliEp -> NodeProxy -> Ep
#[derive(Debug)]
pub struct ClientEndpoint {
    pub(crate) addr: EndpointAddr,
    pub(crate) topic_code: TopicCode,
    pub(crate) interests: HashSet<Interest>,
    pub(crate) node: Weak<ClientNodeInner>,
    pub(crate) message_rx: UnboundedReceiver<Message>,
}
#[derive(Debug, Clone)]
pub struct ClientReceivedMessage {
    ep_addr: EndpointAddr,
    topic_code: TopicCode,
    node: Weak<ClientNodeInner>,
    message: Message,
}

impl Deref for ClientReceivedMessage {
    type Target = Message;
    fn deref(&self) -> &Self::Target {
        &self.message
    }
}

impl DerefMut for ClientReceivedMessage {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.message
    }
}

impl ClientReceivedMessage {
    pub fn into_inner(self) -> Message {
        self.message
    }
    pub async fn ack_failed(&self) -> Result<(), ClientNodeError> {
        let Some(node) = self.node.upgrade() else {
            return Err(ClientNodeError::disconnected());
        };
        let ack = self
            .message
            .header
            .ack_failed(self.topic_code.clone(), self.ep_addr);
        node.send_single_ack(ack).await
    }
    pub async fn ack_processed(&self) -> Result<(), ClientNodeError> {
        let Some(node) = self.node.upgrade() else {
            return Err(ClientNodeError::disconnected());
        };
        let ack = self
            .message
            .header
            .ack_processed(self.topic_code.clone(), self.ep_addr);
        node.send_single_ack(ack).await
    }
    pub async fn ack_received(&self) -> Result<(), ClientNodeError> {
        let Some(node) = self.node.upgrade() else {
            return Err(ClientNodeError::disconnected());
        };
        let ack = self
            .message
            .header
            .ack_received(self.topic_code.clone(), self.ep_addr);
        node.send_single_ack(ack).await
    }
}

impl ClientEndpoint {
    pub fn node(&self) -> Option<ClientNode> {
        self.node.upgrade().map(|inner| ClientNode { inner })
    }
    pub fn interests(&self) -> &HashSet<Interest> {
        &self.interests
    }
    pub async fn modify_interests(
        &mut self,
        modify: impl FnOnce(&mut HashSet<Interest>),
    ) -> Result<(), ClientNodeError> {
        let mut new_interest = self.interests.clone();
        modify(&mut new_interest);
        self.update_interests(new_interest).await
    }
    pub async fn update_interests(
        &mut self,
        interests: impl IntoIterator<Item = Interest>,
    ) -> Result<(), ClientNodeError> {
        let Some(node) = self.node.upgrade() else {
            return Err(ClientNodeError::disconnected());
        };
        let interests_vec: Vec<_> = interests.into_iter().collect();
        let interests_set = interests_vec.iter().cloned().collect::<HashSet<_>>();
        node.send_ep_interests(self.topic_code.clone(), self.addr, interests_vec)
            .await?;
        self.interests = interests_set;
        Ok(())
    }
    pub async fn next_message(&mut self) -> Option<ClientReceivedMessage> {
        self.message_rx
            .recv()
            .await
            .map(|message| ClientReceivedMessage {
                ep_addr: self.addr,
                topic_code: self.topic_code.clone(),
                node: self.node.clone(),
                message,
            })
    }

    pub async fn respawn(&mut self) -> Result<(), ClientNodeError> {
        let Some(node) = self.node.upgrade() else {
            return Err(ClientNodeError::disconnected());
        };
        let new_addr = node
            .send_ep_online(EdgeEndpointOnline {
                topic_code: self.topic_code.clone(),
                interests: self.interests.iter().cloned().collect(),
            })
            .await?;
        self.addr = new_addr;
        Ok(())
    }

    pub async fn next_message_and_auto_respawn(
        &mut self,
    ) -> Result<ClientReceivedMessage, ClientNodeError> {
        loop {
            let message = self.next_message().await;
            let message = match message {
                Some(message) => message,
                None => {
                    self.respawn().await?;
                    continue;
                }
            };
            return Ok(message);
        }
    }
}

impl Drop for ClientEndpoint {
    fn drop(&mut self) {
        let Some(node) = self.node.upgrade() else {
            return;
        };
        let topic_code = self.topic_code.clone();
        let endpoint = self.addr;
        let task = async move {
            let _ = node.send_ep_offline(topic_code, endpoint).await;
            node.endpoint_map.write().await.remove(&endpoint);
        }
        .instrument(
            tracing::info_span!("ep_offline", topic_code = %self.topic_code, endpoint = ?endpoint),
        );
        tokio::spawn(task);
    }
}

#[derive(Debug)]
pub(crate) struct EndpointMailbox {
    pub(crate) message_tx: UnboundedSender<Message>,
    pub(crate) message_rx: Option<UnboundedReceiver<Message>>,
}

impl EndpointMailbox {
    pub fn new() -> Self {
        let (message_tx, message_rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            message_tx,
            message_rx: Some(message_rx),
        }
    }

    pub fn take_rx(&mut self) -> Option<UnboundedReceiver<Message>> {
        self.message_rx.take()
    }
}
