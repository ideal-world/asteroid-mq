use std::collections::HashMap;

use crate::{impl_codec, protocol::{interest::Interest, node::NodeId, topic::TopicCode}, TimestampSec};

use super::{EndpointAddr, Message, MessageId, MessageStatusKind};

#[derive(Debug)]
pub struct DelegateMessage {
    pub message: Message,
}
impl_codec!(
    struct DelegateMessage {
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
pub struct SetState {
    pub message_id: MessageId,
    pub topic: TopicCode,
    pub status: HashMap<EndpointAddr, MessageStatusKind>
}

impl_codec!(
    struct SetState {
        message_id: MessageId,
        topic: TopicCode,
        status: HashMap<EndpointAddr, MessageStatusKind>
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
