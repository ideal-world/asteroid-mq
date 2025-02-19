use asteroid_mq_model::NodeId;
use serde::{Deserialize, Serialize};

use crate::prelude::{Message, TopicCode};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DelegateMessage {
    pub topic: TopicCode,
    pub message: Message,
    pub source: NodeId,
}
