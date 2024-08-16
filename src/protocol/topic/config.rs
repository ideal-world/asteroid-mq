use std::num::NonZeroU32;

use crate::impl_codec;

use super::TopicCode;

#[derive(Debug, Clone, Default)]

pub struct TopicDurabilityConfig {}
#[derive(Debug, Clone, Copy)]
pub enum OverflowPolicy {
    RejectNew = 0,
    DropOld = 1,
}

impl_codec!(
    enum OverflowPolicy {
        RejectNew = 0,
        DropOld = 1,
    }
);
#[derive(Debug, Clone)]
pub struct TopicOverflowConfig {
    pub policy: OverflowPolicy,
    pub size: NonZeroU32,
}
impl_codec!(
    struct TopicOverflowConfig {
        policy: OverflowPolicy,
        size: NonZeroU32,
    }
);

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
    pub overflow_config: Option<TopicOverflowConfig>,
    // pub durability_service: Option<DurabilityService>,
}

impl_codec!(
    struct TopicConfig {
        code: TopicCode,
        blocking: bool,
        overflow_config: Option<TopicOverflowConfig>,
    }
);

impl From<TopicCode> for TopicConfig {
    fn from(code: TopicCode) -> Self {
        Self {
            code,
            blocking: false,
            overflow_config: None,
        }
    }
}
