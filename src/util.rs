use std::{fmt::Write, hash::Hash};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub fn timestamp_sec() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time never goes backward")
        .as_secs()
}

pub fn executor_digest() -> u64 {
    thread_local! {
        static MACH_ID: std::cell::OnceCell<u64> = const { std::cell::OnceCell::new() };
    }
    MACH_ID.with(|t| {
        *t.get_or_init(|| {
            let thread = std::thread::current().id();
            let mach = machine_uid::get()
                .unwrap_or_else(|_| std::env::var("MACHINE_ID").expect("Cannot get machine id"));
            let mut hasher = std::hash::DefaultHasher::new();
            std::hash::Hash::hash(&thread, &mut hasher);
            std::hash::Hash::hash(&mach, &mut hasher);

            std::hash::Hasher::finish(&hasher)
        })
    })
}

pub fn hex<B: AsRef<[u8]> + ?Sized>(bytes: &B) -> Hex<'_> {
    Hex(bytes.as_ref())
}

pub struct Hex<'a>(&'a [u8]);

impl<'a> std::fmt::Debug for Hex<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(f, "{:02x}", byte)?;
        }
        Ok(())
    }
}

impl<'a> std::fmt::Display for Hex<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(f, "{:02x}", byte)?;
        }
        Ok(())
    }
}

pub fn dashed<I: std::fmt::Debug>(arr: &impl AsRef<[I]>) -> Dashed<'_, I> {
    Dashed(arr.as_ref())
}

pub struct Dashed<'a, I>(&'a [I]);

impl<I> std::fmt::Debug for Dashed<'_, I>
where
    I: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let size = self.0.len();
        for (index, i) in self.0.iter().enumerate() {
            f.write_fmt(format_args!("{:?}", i))?;
            if index + 1 != size {
                f.write_char('-')?
            }
        }
        Ok(())
    }
}
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
