use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use typeshare::typeshare;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[typeshare]
pub struct MessageDurableConfig {
    // we should have a expire time
    pub expire: DateTime<Utc>,
    // once it reached the max_receiver, it will be removed
    pub max_receiver: Option<u32>,
}

impl MessageDurableConfig {
    pub fn new(expire: DateTime<Utc>) -> Self {
        Self {
            expire,
            max_receiver: None,
        }
    }
    pub fn with_max_receiver(mut self, max_receiver: u32) -> Self {
        self.max_receiver = Some(max_receiver);
        self
    }
    pub fn with_expire(mut self, expire: DateTime<Utc>) -> Self {
        self.expire = expire;
        self
    }
}
