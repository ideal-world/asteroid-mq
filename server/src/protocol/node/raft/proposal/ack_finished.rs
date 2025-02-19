use asteroid_mq_model::{MessageId, TopicCode};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AckFinished {
    pub message_id: MessageId,
    pub topic: TopicCode,
}
