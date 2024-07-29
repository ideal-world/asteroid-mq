use bytes::Bytes;

use crate::{endpoint::EndpointAddr, interest::Interest};
pub enum MessageAckKind {
    Received = 0,
    Processed = 1,
    Failed = 2,
}
#[derive(Debug, Clone, Copy, Hash)]

pub struct MessageId {
    pub bytes: [u8; 16],
}
#[derive(Debug, Clone)]
pub struct Message {
    pub message_id: MessageId,
    pub target: Target,
    pub payload: Bytes,
}

pub struct MessageAck {
    ack_to: MessageId,
    kind: MessageAckKind,
}
#[derive(Debug, Clone)]
pub enum Target {
    Durable(DurableTarget),
    Online(OnlineTarget),
    Available(AvailableTarget),
    Push(PushTarget),
}
#[derive(Debug, Clone)]

pub struct DurableTarget {
    pub interests: Vec<Interest>,
}
#[derive(Debug, Clone)]

pub struct OnlineTarget {
    pub interests: Vec<Interest>,
}
#[derive(Debug, Clone)]

pub struct AvailableTarget {
    pub interests: Vec<Interest>,
}
#[derive(Debug, Clone)]

pub struct PushTarget {
    pub interests: Vec<Interest>,
}
