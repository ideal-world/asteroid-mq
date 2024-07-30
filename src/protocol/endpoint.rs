use bytes::Bytes;
pub mod event;
use crate::{
    endpoint::EndpointAddr,
    interest::{Interest, Subject},
};

use super::node::NodeId;
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum NNMessagePayloadKind {
    /// Hold Message: edge node ask cluster node to hold a message.
    HoldMessage = 0x10,
    /// Ack: ack to the holder node.
    Ack = 0x11,
    /// Ack Report: The delegate cluster node report ack status to the edge node.
    AckReport = 0x12,
    /// En Online: report endpoint online.
    EpOnline = 0x20,
    /// En Online: report endpoint offline.
    EpOffline = 0x21,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MessageAckKind {
    Received = 0,
    Processed = 1,
    Failed = 2,
}

impl MessageAckKind {
    pub fn is_reached(&self, condition: MessageAckKind) -> bool {
        match self {
            MessageAckKind::Received => {
                condition == MessageAckKind::Received
                    || MessageAckKind::Received == MessageAckKind::Processed
            }
            MessageAckKind::Processed => condition == MessageAckKind::Received,
            MessageAckKind::Failed => false,
        }
    }
    pub fn is_failed(&self) -> bool {
        *self == MessageAckKind::Failed
    }
}
#[derive(Clone, Copy, Hash, PartialEq, Eq)]

pub struct MessageId {
    pub bytes: [u8; 16],
}

impl std::fmt::Debug for MessageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("MessageId")
            .field(&crate::util::dashed(&[
                crate::util::hex(&self.bytes[0..4]),
                crate::util::hex(&self.bytes[4..12]),
                crate::util::hex(&self.bytes[12..16]),
            ]))
            .finish()
    }
}

impl MessageId {
    pub fn random() -> Self {
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
        bytes[0..4].copy_from_slice(&eid.to_be_bytes());
        bytes[4..12].copy_from_slice(&timestamp.to_be_bytes());
        bytes[12..16].copy_from_slice(&counter.to_be_bytes());
        Self { bytes }
    }
}

#[derive(Debug, Clone)]
pub struct Message {
    pub header: MessageHeader,
    pub payload: Bytes,
}
impl Message {
    pub fn id(&self) -> MessageId {
        self.header.message_id
    }
    pub fn ack_kind(&self) -> Option<MessageAckKind> {
        self.header.ack_kind
    }
}
#[derive(Debug, Clone)]
pub struct MessageHeader {
    pub message_id: MessageId,
    pub holder_node: NodeId,
    pub ack_kind: Option<MessageAckKind>,
    pub target: MessageTarget,
}

pub struct MessageAck {
    pub ack_to: MessageId,
    pub holder: NodeId,
    pub kind: MessageAckKind,
}
#[derive(Debug, Clone)]
pub enum MessageTarget {
    Durable(DurableTarget),
    Online(OnlineTarget),
    Available(AvailableTarget),
    Push(PushTarget),
}

impl MessageTarget {
    pub fn push<I: IntoIterator<Item = Subject>>(subjects: I) -> Self {
        Self::Push(PushTarget {
            subjects: subjects.into_iter().collect(),
        })
    }
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
    pub subjects: Vec<Subject>,
}
