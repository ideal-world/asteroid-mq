use std::collections::HashMap;

use chrono::{DateTime, Utc};

use crate::protocol::endpoint::{EndpointAddr, Message, MessageAckKind};

pub struct DurableMessage {
    pub message: Message,
    pub config: DurableMessageConfig,
    pub status: HashMap<EndpointAddr, MessageAckKind>,
}

pub struct DurableMessageConfig {
    // we should have a expire time
    pub expire: DateTime<Utc>,
    // once it reached the max_receiver, it will be removed
    pub max_receiver: Option<u32>,
}
