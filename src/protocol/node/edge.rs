use std::{borrow::Cow, sync::Arc};

use bytes::Bytes;

use crate::{
    impl_codec,
    prelude::{
        CodecType, MessageAckExpectKind, MessageDurabilityConfig, MessageId, Subject, TopicCode,
    },
    protocol::endpoint::{Message, MessageHeader, MessageTargetKind},
};

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum EdgeRequestKind {
    SendMessage = 0x10,
    CreateEndpoint = 0x11,
    DeleteEndpoint = 0x12,
    Ack = 0x13,
}
impl_codec!(
    enum EdgeRequestKind {
        SendMessage = 0x10,
        CreateEndpoint = 0x11,
        DeleteEndpoint = 0x12,
        Ack = 0x13,
    }
);

pub struct EdgePayload {
    pub kind: EdgeRequestKind,
    pub data: Bytes,
}

impl_codec!(
    struct EdgePayload {
        kind: EdgeRequestKind,
        data: Bytes,
    }
);

pub struct EdgeRequest {
    pub id: u64,
    pub kind: EdgeRequestKind,
    pub data: Bytes,
}

impl_codec!(
    struct EdgeRequest {
        id: u64,
        kind: EdgeRequestKind,
        data: Bytes,
    }
);

pub struct EdgeResponse {
    pub id: u64,
    pub result: Bytes,
}

impl_codec!(
    struct EdgeResponse {
        id: u64,
        result: Bytes,
    }
);

impl EdgeResponse {
    pub fn from_result<T: CodecType>(id: u64, result: Result<T, EdgeError>) -> Self {
        Self {
            id,
            result: result.encode_to_bytes(),
        }
    }
}

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
#[derive(Debug, Clone, Copy)]
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
