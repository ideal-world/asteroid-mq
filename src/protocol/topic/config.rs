use std::num::NonZeroU32;

use super::{durable_message::DurabilityService, TopicCode};
#[derive(Debug, Clone, Default)]

pub struct TopicDurabilityConfig {}
#[derive(Debug, Clone, Copy)]
pub enum OverflowPolicy {
    RejectNew,
    DropOld,
}
#[derive(Debug, Clone)]
pub struct TopicOverflowConfig {
    pub policy: OverflowPolicy,
    pub size: NonZeroU32,
}

impl TopicOverflowConfig {
    #[inline(always)]
    pub fn size(&self) -> usize {
        self.size.get() as usize
    }
}

#[derive(Debug, Clone)]
pub struct TopicConfig {
    pub code: TopicCode,
    pub blocking: bool,
    pub overflow: Option<TopicOverflowConfig>,
    pub durability: Option<DurabilityService>,
}

impl From<TopicCode> for TopicConfig {
    fn from(code: TopicCode) -> Self {
        Self {
            code,
            blocking: false,
            overflow: None,
            durability: None,
        }
    }
}
