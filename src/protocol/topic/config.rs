use std::{borrow::Cow, future::Future, ops::RangeToInclusive};

use chrono::{DateTime, Utc};

use crate::protocol::endpoint::{Message, MessageId};

use super::durable_message::TopicDurabilityConfig;

pub struct TopicConfig {
    blocking: bool,
    size: Option<u32>,
    durability: TopicDurabilityConfig,
}

