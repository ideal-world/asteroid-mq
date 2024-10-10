use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use typeshare::typeshare;

use crate::{
    endpoint::EndpointAddr,
    interest::Interest,
    message::{MessageId, MessageStatusKind},
    topic::TopicCode,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[typeshare]
pub struct EndpointInterest {
    pub topic_code: TopicCode,
    pub endpoint: EndpointAddr,
    pub interests: Vec<Interest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[typeshare]
pub struct SetState {
    pub topic: TopicCode,
    pub update: MessageStateUpdate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[typeshare]
pub struct MessageStateUpdate {
    pub message_id: MessageId,
    pub status: HashMap<EndpointAddr, MessageStatusKind>,
}

impl MessageStateUpdate {
    pub fn new(message_id: MessageId, status: HashMap<EndpointAddr, MessageStatusKind>) -> Self {
        Self { message_id, status }
    }
    pub fn new_empty(message_id: MessageId) -> Self {
        Self {
            message_id,
            status: HashMap::new(),
        }
    }
}
