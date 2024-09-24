use serde::{Deserialize, Serialize};

use crate::prelude::TopicCode;

#[derive(Debug, Clone, Serialize, Deserialize)]

pub struct UnloadTopic {
    pub code: TopicCode,
}

impl UnloadTopic {
    pub fn new(code: TopicCode) -> Self {
        Self { code }
    }
}
