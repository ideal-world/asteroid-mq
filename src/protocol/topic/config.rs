use std::num::NonZeroU32;

use crate::impl_codec;

use super::TopicCode;

#[derive(Debug, Clone, Default)]

pub struct TopicDurabilityConfig {}
#[derive(Debug, Clone, Copy, Default)]
pub enum TopicOverflowPolicy {
    #[default]
    RejectNew = 0,
    DropOld = 1,
}

impl_codec!(
    enum TopicOverflowPolicy {
        RejectNew = 0,
        DropOld = 1,
    }
);
#[derive(Debug, Clone)]
pub struct TopicOverflowConfig {
    pub policy: TopicOverflowPolicy,
    pub size: NonZeroU32,
}
impl_codec!(
    struct TopicOverflowConfig {
        policy: TopicOverflowPolicy,
        size: NonZeroU32,
    }
);

impl TopicOverflowConfig {
    #[inline(always)]
    pub fn size(&self) -> usize {
        self.size.get() as usize
    }
    pub fn new_reject_new(size: u32) -> Self {
        Self {
            policy: TopicOverflowPolicy::RejectNew,
            size: NonZeroU32::new(size).unwrap_or(NonZeroU32::MAX),
        }
    }
    pub fn new_drop_old(size: u32) -> Self {
        Self {
            policy: TopicOverflowPolicy::DropOld,
            size: NonZeroU32::new(size).unwrap_or(NonZeroU32::MAX),
        }
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
