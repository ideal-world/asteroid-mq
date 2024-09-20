use serde::{Deserialize, Serialize};
use typeshare::typeshare;

use crate::prelude::{EndpointAddr, Interest, TopicCode};
#[derive(Debug, Clone, Serialize, Deserialize)]
#[typeshare]
pub struct EndpointInterest {
    pub topic_code: TopicCode,
    pub endpoint: EndpointAddr,
    pub interests: Vec<Interest>,
}
