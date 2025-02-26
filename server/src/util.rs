use std::hash::{BuildHasher, Hash, Hasher};

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

// MurmurHash64A 哈希器实现
#[derive(Clone)]
pub struct MurmurHasher64A {
    h: u64,            // 当前哈希值
    buffer: [u8; 8],   // 缓冲区，用于处理未对齐的字节
    buffer_len: usize, // 缓冲区中的字节数
    total_len: u64,    // 总共处理的字节数
}
impl Default for MurmurHasher64A {
    fn default() -> Self {
        Self::new_fixed()
    }
}
impl MurmurHasher64A {
    /// just a random number I typed randomly
    const DEFAULT_SEED: u64 = 0x2F33_E601_4295_77A3;
    pub fn new_fixed() -> Self {
        Self::new(Self::DEFAULT_SEED)
    }
    // 创建一个新的 MurmurHasher64A 实例
    pub fn new(seed: u64) -> Self {
        MurmurHasher64A {
            h: seed,
            buffer: [0; 8],
            buffer_len: 0,
            total_len: 0,
        }
    }

    const M: u64 = 0xc6a4a7935bd1e995;
    const R: u64 = 47;
    // 处理完整的8字节块
    fn process_block(&mut self, block: &[u8; 8]) {
        let k = u64::from_le_bytes(*block);
        let k = k.wrapping_mul(Self::M);
        let k = k ^ (k >> Self::R);
        let k = k.wrapping_mul(Self::M);
        self.h = self.h.wrapping_mul(Self::M);
        self.h ^= k;
    }

    // 处理缓冲区中的尾部字节
    fn process_tail(&mut self) {
        if self.buffer_len > 0 {
            let mut k = 0u64;
            for i in 0..self.buffer_len {
                k |= (self.buffer[i] as u64) << (i * 8);
            }
            let k = k.wrapping_mul(Self::M);
            let k = k ^ (k >> Self::R);
            let k = k.wrapping_mul(Self::M);
            self.h ^= k;
        }
    }

    // 最终化哈希值
    fn finalize_hash(&self) -> u64 {
        let mut h = self.h;
        h ^= self.total_len;
        h ^= h >> 33;
        h = h.wrapping_mul(0xff51afd7ed558ccd);
        h ^= h >> 33;
        h = h.wrapping_mul(0xc4ceb9fe1a85ec53);
        h ^= h >> 33;
        h
    }
}

impl Hasher for MurmurHasher64A {
    fn write(&mut self, bytes: &[u8]) {
        let mut bytes = bytes;
        self.total_len += bytes.len() as u64;

        // 如果缓冲区有数据，先尝试填满缓冲区
        if self.buffer_len > 0 {
            let to_copy = std::cmp::min(8 - self.buffer_len, bytes.len());
            self.buffer[self.buffer_len..self.buffer_len + to_copy]
                .copy_from_slice(&bytes[..to_copy]);
            self.buffer_len += to_copy;
            bytes = &bytes[to_copy..];

            if self.buffer_len == 8 {
                self.process_block(&self.buffer.clone());
                self.buffer_len = 0;
            }
        }

        // 处理完整的8字节块
        while bytes.len() >= 8 {
            let block: [u8; 8] = bytes[..8].try_into().unwrap();
            self.process_block(&block);
            bytes = &bytes[8..];
        }

        // 将剩余字节存入缓冲区
        if !bytes.is_empty() {
            self.buffer[..bytes.len()].copy_from_slice(bytes);
            self.buffer_len = bytes.len();
        }
    }

    fn finish(&self) -> u64 {
        let mut hasher = self.clone();
        hasher.process_tail();
        hasher.finalize_hash()
    }
}

#[derive(Debug, Clone, Default)]
pub struct FixedHash;

impl BuildHasher for FixedHash {
    type Hasher = MurmurHasher64A;
    fn build_hasher(&self) -> Self::Hasher {
        MurmurHasher64A::new_fixed()
    }
}

pub fn hash64<T: Hash>(value: &T) -> u64 {
    use std::hash::Hasher;
    let mut hasher = MurmurHasher64A::new_fixed();
    value.hash(&mut hasher);
    Hasher::finish(&hasher)
}

pub const KB: usize = 1024;
pub const MB: usize = 1024 * KB;
