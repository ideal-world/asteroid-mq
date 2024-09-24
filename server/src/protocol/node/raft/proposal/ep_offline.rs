use serde::{Deserialize, Serialize};

use crate::prelude::{EndpointAddr, NodeId, TopicCode};

#[derive(Debug, Clone, Serialize, Deserialize)]

pub struct EndpointOffline {
    pub topic_code: TopicCode,
    pub endpoint: EndpointAddr,
    pub host: NodeId,
}
