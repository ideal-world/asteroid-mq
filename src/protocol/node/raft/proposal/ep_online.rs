use serde::{Deserialize, Serialize};

use crate::prelude::{EndpointAddr, Interest, NodeId, TopicCode};

#[derive(Debug, Clone, Serialize, Deserialize)]

pub struct EndpointOnline {
    pub topic_code: TopicCode,
    pub endpoint: EndpointAddr,
    pub interests: Vec<Interest>,
    pub host: NodeId,
}
