use std::borrow::Cow;

use bytes::Bytes;

use crate::{impl_codec, prelude::CodecType};

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
    Internal = 0x01,
}

impl_codec!(
    enum EdgeErrorKind {
        Decode = 0x00,
        Internal = 0x01,
    }
);
