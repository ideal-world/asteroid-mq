use bytes::Bytes;

use crate::EndpointAddr;

pub enum MessageAck {
    Received = 0,
    Processed = 1,
    Failed = 2,
}

pub enum MessageStatus {
    ReadyToSend = 0,
    Sent = 1,
    Processed = 2,
    ReadyToDelete = 3,
}

pub struct NodeHoldingToEpMessage {
    pub source_message_id: u64,
    pub target_message_id: u64,
    pub source: EndpointAddr,
    pub target: EndpointAddr,
    pub payload: Bytes,
}

pub struct ToEpMessage {
    pub source_message_id: u64,
    pub target_message_id: u64,
    pub source: EndpointAddr,
    pub target: EndpointAddr,
    pub status: MessageStatus,
}

