use crate::{impl_codec, interest::Interest, protocol::node::NodeId, TimestampSec};

use super::{EndpointAddr, Message};

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
    pub endpoint: EndpointAddr,
    pub interests: Vec<Interest>,
    pub host: NodeId,
}

impl_codec!(
    struct EndpointOnline {
        endpoint: EndpointAddr,
        host: NodeId,
        interests: Vec<Interest>,
    }
);
#[derive(Debug)]
pub struct EndpointOffline {
    pub endpoint: EndpointAddr,
    pub host: NodeId,
}

impl_codec!(
    struct EndpointOffline {
        endpoint: EndpointAddr,
        host: NodeId,
    }
);
#[derive(Debug)]
pub struct EndpointSync {
    pub routing: Vec<EpInfo>,
}

impl_codec!(
    struct EndpointSync {
        routing: Vec<EpInfo>,
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
