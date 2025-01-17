use std::{borrow::Borrow, collections::HashMap};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use typeshare::typeshare;

use crate::{endpoint::EndpointAddr, message::MessageStatusKind};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[typeshare(serialized_as = "String")]
/// code are expect to be a valid utf8 string
pub struct TopicCode(pub(crate) Bytes);
impl TopicCode {
    pub fn new<B: Into<String>>(code: B) -> Self {
        Self(Bytes::from(code.into()))
    }
    pub const fn const_new(code: &'static str) -> Self {
        Self(Bytes::from_static(code.as_bytes()))
    }
}

impl From<&'_ str> for TopicCode {
    fn from(val: &'_ str) -> Self {
        TopicCode::new(val)
    }
}

impl From<String> for TopicCode {
    fn from(val: String) -> Self {
        TopicCode::new(val)
    }
}

impl From<&'_ [u8]> for TopicCode {
    fn from(val: &'_ [u8]) -> Self {
        TopicCode(Bytes::copy_from_slice(val))
    }
}

impl From<Vec<u8>> for TopicCode {
    fn from(val: Vec<u8>) -> Self {
        TopicCode(Bytes::from(val))
    }
}

impl Serialize for TopicCode {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let string = unsafe { std::str::from_utf8_unchecked(self.0.as_ref()) };
        serializer.serialize_str(string)
    }
}

impl<'de> Deserialize<'de> for TopicCode {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let string = String::deserialize(deserializer)?;
        Ok(Self(Bytes::from(string)))
    }
}

impl Borrow<[u8]> for TopicCode {
    fn borrow(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Display for TopicCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        unsafe { f.write_str(std::str::from_utf8_unchecked(&self.0)) }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "bincode", derive(bincode::Decode, bincode::Encode))]
#[typeshare]
pub struct WaitAckError {
    pub status: HashMap<EndpointAddr, MessageStatusKind>,
    pub exception: Option<WaitAckErrorException>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "bincode", derive(bincode::Decode, bincode::Encode))]
#[typeshare]
pub struct WaitAckSuccess {
    pub status: HashMap<EndpointAddr, MessageStatusKind>,
}

impl WaitAckError {
    pub fn exception(exception: WaitAckErrorException) -> Self {
        Self {
            status: HashMap::new(),
            exception: Some(exception),
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[cfg_attr(feature = "bincode", derive(bincode::Decode, bincode::Encode))]
#[typeshare]
pub enum WaitAckErrorException {
    MessageDropped = 0,
    Overflow = 1,
    NoAvailableTarget = 2,
}

pub enum AckWaitErrorKind {
    Timeout,
    Fail,
}
pub type WaitAckResult = Result<WaitAckSuccess, WaitAckError>;
