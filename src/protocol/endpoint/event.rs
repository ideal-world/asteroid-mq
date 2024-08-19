use std::collections::HashMap;

use crate::{
    impl_codec,
    protocol::{interest::Interest, node::NodeId, topic::TopicCode},
    TimestampSec,
};

use super::{EndpointAddr, Message, MessageId, MessageStatusKind};

#[derive(Debug)]
pub struct DelegateMessage {
    pub topic: TopicCode,
    pub message: Message,
}
impl_codec!(
    struct DelegateMessage {
        topic: TopicCode,
        message: Message,
    }
);
#[derive(Debug)]

pub struct CastMessage {
    pub target_eps: Vec<EndpointAddr>,
    pub message: Message,
}
impl_codec!(
    struct CastMessage {
        target_eps: Vec<EndpointAddr>,
        message: Message,
    }
);

#[derive(Debug)]

pub struct EndpointOnline {
    pub topic_code: TopicCode,
    pub endpoint: EndpointAddr,
    pub interests: Vec<Interest>,
    pub host: NodeId,
}

impl_codec!(
    struct EndpointOnline {
        endpoint: EndpointAddr,
        topic_code: TopicCode,
        host: NodeId,
        interests: Vec<Interest>,
    }
);
#[derive(Debug)]
pub struct EndpointOffline {
    pub topic_code: TopicCode,
    pub endpoint: EndpointAddr,
    pub host: NodeId,
}

impl_codec!(
    struct EndpointOffline {
        topic_code: TopicCode,
        endpoint: EndpointAddr,
        host: NodeId,
    }
);

#[derive(Debug)]
pub struct EndpointInterest {
    pub topic_code: TopicCode,
    pub endpoint: EndpointAddr,
    pub interests: Vec<Interest>,
}

impl_codec!(
    struct EndpointInterest {
        topic_code: TopicCode,
        endpoint: EndpointAddr,
        interests: Vec<Interest>,
    }
);

#[derive(Debug)]
pub struct MessageStateUpdate {
    pub message_id: MessageId,
    pub status: HashMap<EndpointAddr, MessageStatusKind>,
}

impl_codec!(
    struct MessageStateUpdate {
        message_id: MessageId,
        status: HashMap<EndpointAddr, MessageStatusKind>,
    }
);

impl MessageStateUpdate {
    pub fn new(message_id: MessageId, status: HashMap<EndpointAddr, MessageStatusKind>) -> Self {
        Self { message_id, status }
    }
    pub fn new_empty(message_id: MessageId) -> Self {
        Self {
            message_id,
            status: HashMap::new(),
        }
    }
}

#[derive(Debug)]
pub struct SetState {
    pub topic: TopicCode,
    pub update: MessageStateUpdate,
}

impl_codec!(
    struct SetState {
        topic: TopicCode,
        update: MessageStateUpdate,
    }
);

#[derive(Debug)]
pub struct EndpointSync {
    pub entries: Vec<(TopicCode, Vec<EpInfo>)>,
}

impl_codec!(
    struct EndpointSync {
        entries: Vec<(TopicCode, Vec<EpInfo>)>,
    }
);

#[derive(Debug)]
pub struct EpInfo {
    pub addr: EndpointAddr,
    pub host: NodeId,
    pub interests: Vec<Interest>,
    pub latest_active: TimestampSec,
}

impl_codec!(
    struct EpInfo {
        addr: EndpointAddr,
        host: NodeId,
        interests: Vec<Interest>,
        latest_active: TimestampSec,
    }
);
