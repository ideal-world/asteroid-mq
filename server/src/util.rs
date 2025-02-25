use std::hash::Hash;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A data with utc time
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Timed<T> {
    pub time: DateTime<Utc>,
    pub data: T,
}

impl<T> Timed<T> {
    pub fn new(time: DateTime<Utc>, data: T) -> Self {
        Self { time, data }
    }
}

impl<T: PartialEq> PartialEq for Timed<T> {
    fn eq(&self, other: &Self) -> bool {
        self.time == other.time && self.data == other.data
    }
}

impl<T: Eq> Eq for Timed<T> {}

impl<T: PartialEq> PartialOrd for Timed<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.data != other.data {
            None
        } else {
            self.time.partial_cmp(&other.time)
        }
    }
}
impl<T: Eq> Ord for Timed<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.time.cmp(&other.time)
    }
}

pub fn hash64<T: Hash>(value: &T) -> u64 {
    use std::hash::{DefaultHasher, Hasher};
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    Hasher::finish(&hasher)
}

pub const KB: usize = 1024;
pub const MB: usize = 1024 * KB;
