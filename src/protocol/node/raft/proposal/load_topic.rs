use serde::{Deserialize, Serialize};

use crate::{
    prelude::DurableMessage, protocol::node::raft::state_machine::topic::config::TopicConfig,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadTopic {
    pub config: TopicConfig,
    pub queue: Vec<DurableMessage>,
}

impl LoadTopic {
    pub fn from_config<C: Into<TopicConfig>>(config: C) -> Self {
        Self {
            config: config.into(),
            queue: Vec::new(),
        }
    }
}
