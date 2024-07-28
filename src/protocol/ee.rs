use bytes::Bytes;

use crate::endpoint::EndpointAddr;
pub enum MessageAckKind {
    Received = 0,
    Processed = 1,
    Failed = 2,
}

pub struct MessageId {
    pub bytes: [u8; 16],
}

pub struct Message {
    message_id: MessageId,
    target: Target,
    payload: Bytes,
}

pub struct MessageAck {
    ack_to: MessageId,
    kind: MessageAckKind,
}

pub enum Target {
    Durable(DurableTarget),
    Online(OnlineTarget),
    Available(AvailableTarget),
    Push(PushTarget),
}

pub struct DurableTarget {
    pub interests: Vec<Interest>,
}
pub struct OnlineTarget {
    pub interests: Vec<Interest>,
}
pub struct AvailableTarget {
    pub interests: Vec<Interest>,
}
pub struct PushTarget {
    pub interests: Vec<Interest>,
}
pub struct Interest(Bytes);
