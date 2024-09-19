use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};

use crate::{prelude::TopicCode};

#[derive(Debug, Clone, Default)]

pub struct TopicDurabilityConfig {}
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub enum TopicOverflowPolicy {
    #[default]
    RejectNew = 0,
    DropOld = 1,
}


#[derive(Debug, Clone, Serialize, Deserialize)]

pub struct TopicOverflowConfig {
    pub policy: TopicOverflowPolicy,
    pub size: NonZeroU32,
}


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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicConfig {
    pub code: TopicCode,
    pub blocking: bool,
    pub overflow_config: Option<TopicOverflowConfig>,
}



impl From<TopicCode> for TopicConfig {
    fn from(code: TopicCode) -> Self {
        Self {
            code,
            blocking: false,
            overflow_config: None,
        }
    }
}
