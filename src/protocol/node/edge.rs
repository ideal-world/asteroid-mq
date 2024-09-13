use std::{borrow::Cow, sync::Arc};

use bytes::Bytes;
pub mod codec;
pub mod connection;
pub mod packet;
use crate::{
    impl_codec,
    prelude::{
        CodecType, MessageAckExpectKind, MessageDurabilityConfig, MessageId, Subject, TopicCode,
    },
    protocol::endpoint::{Message, MessageHeader, MessageTargetKind},
};

use super::raft::state_machine::topic::wait_ack::WaitAckResult;

#[derive(Debug, Clone)]
#[derive(serde::Serialize, serde::Deserialize)]
#[repr(u8)]
pub enum EdgeRequestKind {
    SendMessage(EdgeMessage),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum EdgePayload {
    Message(EdgeMessage),
    Response(EdgeResponse),
    Request(EdgeRequest),
    Error(EdgeError),
}
pub enum EdgePush {
    Message(Message),
    Ack(WaitAckResult),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]

pub struct EdgeRequest {
    pub seq_id: u64,
    pub kind: EdgeRequestKind,
}


#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EdgeResponse {
    pub id: u64,
    pub result: EdgeResponseKind,
}

impl_codec!(
    struct EdgeResponse {
        id: u64,
        result: Bytes,
    }
);
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum EdgeResponseKind {
    SendMessage(WaitAckResult),
}
impl EdgeResponse {
    pub fn send_message(id: u64, result: WaitAckResult) -> Self {
        Self {
            id,
            result: EdgeResponseKind::SendMessage(result),
        }
    }
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EdgeError {
    pub context: Cow<'static, str>,
    pub message: Option<Cow<'static, str>>,
    pub kind: EdgeErrorKind,
}

impl_codec!(
    struct EdgeError {
        context: Cow<'static, str>,
        message: Option<Cow<'static, str>>,
        kind: EdgeErrorKind,
    }
);

impl EdgeError {
    pub fn new(context: impl Into<Cow<'static, str>>, kind: EdgeErrorKind) -> Self {
        Self {
            context: context.into(),
            message: None,
            kind,
        }
    }
    pub fn with_message(
        context: impl Into<Cow<'static, str>>,
        message: impl Into<Cow<'static, str>>,
        kind: EdgeErrorKind,
    ) -> Self {
        Self {
            context: context.into(),
            message: Some(message.into()),
            kind,
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]

pub enum EdgeErrorKind {
    Decode = 0x00,
    TopicNotFound = 0x02,
    Internal = 0xf0,
}

impl_codec!(
    enum EdgeErrorKind {
        Decode = 0x00,
        TopicNotFound = 0x02,
        Internal = 0xf0,
    }
);
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EdgeMessageHeader {
    pub ack_kind: MessageAckExpectKind,
    pub target_kind: MessageTargetKind,
    pub durability: Option<MessageDurabilityConfig>,
    pub subjects: Vec<Subject>,
    pub topic: TopicCode,
}

impl_codec!(
    struct EdgeMessageHeader {
        ack_kind: MessageAckExpectKind,
        target_kind: MessageTargetKind,
        durability: Option<MessageDurabilityConfig>,
        subjects: Vec<Subject>,
        topic: TopicCode,
    }
);

impl EdgeMessageHeader {
    pub fn into_message_header(self) -> (MessageHeader, TopicCode) {
        (
            MessageHeader {
                message_id: MessageId::new_snowflake(),
                ack_kind: self.ack_kind,
                target_kind: self.target_kind,
                durability: self.durability,
                subjects: self.subjects.into(),
            },
            self.topic,
        )
    }
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EdgeMessage {
    pub header: EdgeMessageHeader,
    pub payload: Bytes,
}

impl_codec!(
    struct EdgeMessage {
        header: EdgeMessageHeader,
        payload: Bytes,
    }
);
impl EdgeMessage {
    pub fn into_message(self) -> (Message, TopicCode) {
        let (header, topic) = self.header.into_message_header();
        (Message::new(header, self.payload), topic)
    }
}
