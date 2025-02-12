use std::{convert::Infallible, fmt::Display, str::FromStr};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use typeshare::typeshare;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[typeshare(serialized_as = "String")]
pub struct Subject(pub(crate) Bytes);

impl From<&'static str> for Subject {
    fn from(value: &'static str) -> Self {
        Subject(Bytes::from_static(value.as_bytes()))
    }
}

impl Serialize for Subject {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let string = unsafe { std::str::from_utf8_unchecked(self.0.as_ref()) };
        serializer.serialize_str(string)
    }
}

impl<'de> Deserialize<'de> for Subject {
    fn deserialize<D>(deserializer: D) -> Result<Subject, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let string = String::deserialize(deserializer)?;
        Ok(Subject(Bytes::from(string)))
    }
}

impl FromStr for Subject {
    type Err = Infallible;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Subject(Bytes::from(s.to_owned())))
    }
}

impl Display for Subject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let string = unsafe { std::str::from_utf8_unchecked(self.0.as_ref()) };
        write!(f, "{}", string)
    }
}

impl AsRef<str> for Subject {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Subject {
    pub fn as_str(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(self.0.as_ref()) }
    }
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_ref()
    }
    pub fn new<B: Into<Bytes>>(bytes: B) -> Self {
        Self(bytes.into())
    }
    pub const fn const_new(bytes: &'static str) -> Self {
        Self(Bytes::from_static(bytes.as_bytes()))
    }
    pub fn segments(&self) -> SubjectSegments<'_> {
        SubjectSegments {
            inner: self.0.as_ref(),
        }
    }
}
#[derive(Debug, Clone)]
pub struct SubjectSegments<'a> {
    // stripped slash, always start with a non-slash byte
    inner: &'a [u8],
}

impl<'a> Iterator for SubjectSegments<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.inner.is_empty() {
            return None;
        }
        let mut slash_index = None;
        for index in 0..self.inner.len() {
            if self.inner[index] == b'/' {
                slash_index.replace(index);
                break;
            }
        }
        if let Some(slash_index) = slash_index {
            let next = &self.inner[0..slash_index];
            let mut next_start = None;
            for (bias, b) in self.inner[slash_index..].iter().enumerate() {
                if *b != b'/' {
                    next_start = Some(slash_index + bias);
                    break;
                }
            }
            if let Some(next_start) = next_start {
                self.inner = &self.inner[next_start..]
            } else {
                self.inner = &[];
            }
            Some(next)
        } else {
            let next = self.inner;
            self.inner = &[];
            Some(next)
        }
    }
}

/// # Interest
/// ## Glob Match Interest
/// (/)?(<path>|<*>|<**>)/*
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[typeshare(serialized_as = "String")]
pub struct Interest(pub(crate) Bytes);

impl Serialize for Interest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let string = unsafe { std::str::from_utf8_unchecked(self.0.as_ref()) };
        serializer.serialize_str(string)
    }
}

impl<'de> Deserialize<'de> for Interest {
    fn deserialize<D>(deserializer: D) -> Result<Interest, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let string = String::deserialize(deserializer)?;
        Ok(Interest(Bytes::from(string)))
    }
}

impl Interest {
    pub fn new<B: Into<Bytes>>(bytes: B) -> Self {
        Self(bytes.into())
    }
    pub fn as_segments(&self) -> impl Iterator<Item = InterestSegment<'_>> + Clone {
        self.0.split(|c| *c == b'/').filter_map(|seg| {
            if seg.is_empty() {
                None
            } else {
                Some(match seg.trim_ascii() {
                    b"*" => InterestSegment::Any,
                    b"**" => InterestSegment::RecursiveAny,
                    specific => InterestSegment::Specific(specific),
                })
            }
        })
    }
}

pub enum InterestSegment<'a> {
    Specific(&'a [u8]),
    Any,
    RecursiveAny,
}
impl InterestSegment<'_> {
    pub fn to_owned(&self) -> OwnedInterestSegment {
        match self {
            InterestSegment::Specific(s) => OwnedInterestSegment::Specific(s.to_vec()),
            InterestSegment::Any => OwnedInterestSegment::Any,
            InterestSegment::RecursiveAny => OwnedInterestSegment::RecursiveAny,
        }
    }
}
pub enum OwnedInterestSegment {
    Specific(Vec<u8>),
    Any,
    RecursiveAny,
}
