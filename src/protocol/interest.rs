//! # Interest
//! ## Match Interest
//! (/)?(<path>|<*>|<**>)/*
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    hash::Hash,
};

use bytes::Bytes;

use crate::impl_codec;

#[derive(Debug, Clone, PartialEq ,Eq, Hash)]
pub struct Subject(pub(crate) Bytes);

impl_codec!(
    struct Subject(Bytes)
);

impl Subject {
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_ref()
    }
    pub fn new<B: Into<Bytes>>(bytes: B) -> Self {
        Self(bytes.into())
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
pub struct Interest(Bytes);

impl_codec!(
    struct Interest(Bytes)
);
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
#[derive(Debug)]
pub struct InterestMap<T> {
    root: InterestRadixTreeNode<T>,
    pub(crate) raw: HashMap<T, HashSet<Interest>>,
}

impl<T> Default for InterestMap<T> {
    fn default() -> Self {
        Self {
            root: Default::default(),
            raw: HashMap::default(),
        }
    }
}

#[derive(Debug)]
pub struct InterestRadixTreeNode<T> {
    value: HashSet<T>,
    children: BTreeMap<Vec<u8>, InterestRadixTreeNode<T>>,
    any_child: Option<Box<InterestRadixTreeNode<T>>>,
    recursive_any_child: Option<Box<InterestRadixTreeNode<T>>>,
}

impl<T> Default for InterestRadixTreeNode<T> {
    fn default() -> Self {
        Self {
            value: HashSet::default(),
            children: BTreeMap::new(),
            any_child: None,
            recursive_any_child: None,
        }
    }
}

impl<T> InterestRadixTreeNode<T>
where
    T: Hash + Eq + PartialEq,
{
    fn insert_recursive<'a>(
        &mut self,
        mut path: impl Iterator<Item = InterestSegment<'a>>,
        value: T,
    ) {
        match path.next() {
            Some(InterestSegment::Specific(seg)) => {
                if let Some(child) = self.children.get_mut(seg) {
                    child.insert_recursive(path, value)
                } else {
                    let mut child_tree = InterestRadixTreeNode::default();
                    child_tree.insert_recursive(path, value);
                    self.children.insert(seg.to_owned(), child_tree);
                }
            }
            Some(InterestSegment::Any) => {
                let child = self.any_child.get_or_insert_with(Default::default);
                child.insert_recursive(path, value)
            }
            Some(InterestSegment::RecursiveAny) => {
                let child = self
                    .recursive_any_child
                    .get_or_insert_with(Default::default);
                child.insert_recursive(path, value)
            }
            None => {
                self.value.insert(value);
            }
        }
    }
    fn delete_recursive<'a>(
        &mut self,
        mut path: impl Iterator<Item = InterestSegment<'a>>,
        value: &T,
    ) {
        match path.next() {
            Some(InterestSegment::Specific(seg)) => {
                if let Some(child) = self.children.get_mut(seg) {
                    child.delete_recursive(path, value)
                }
            }
            Some(InterestSegment::Any) => {
                if let Some(ref mut child) = self.any_child {
                    child.delete_recursive(path, value)
                }
            }
            Some(InterestSegment::RecursiveAny) => {
                if let Some(ref mut child) = self.recursive_any_child {
                    child.delete_recursive(path, value)
                }
            }
            None => {
                self.value.remove(value);
            }
        }
    }
    fn find_all_recursive<'a, 'i>(
        &'a self,
        mut path: impl Iterator<Item = &'i [u8]> + Clone,
        collector: &mut HashSet<&'a T>,
    ) {
        if let Some(seg) = path.next() {
            if let Some(ref rac) = self.recursive_any_child {
                let mut rest_path = path.clone();
                collector.extend(&rac.value);
                while let Some(recursive_seg) = rest_path.next() {
                    if let Some(matched) = rac.children.get(recursive_seg) {
                        matched.find_all_recursive(rest_path.clone(), collector)
                    }
                }
            }
            if let Some(ref ac) = self.any_child {
                ac.find_all_recursive(path.clone(), collector)
            }
            if let Some(child) = self.children.get(seg) {
                child.find_all_recursive(path, collector)
            }
        } else {
            collector.extend(&self.value)
        }
    }
}
impl<T> InterestMap<T>
where
    T: Hash + Eq + PartialEq + Clone,
{
    pub fn new() -> Self {
        Self {
            root: InterestRadixTreeNode::default(),
            raw: HashMap::default(),
        }
    }
    pub fn from_raw(raw: HashMap<T, HashSet<Interest>>) -> Self {
        let mut map = Self::new();
        for (value, interests) in raw {
            for interest in &interests {
                map.root
                    .insert_recursive(interest.as_segments(), value.clone());
            }
            map.raw.insert(value, interests);
        }
        map
    }

    pub fn insert(&mut self, interest: Interest, value: T) {
        self.root
            .insert_recursive(interest.as_segments(), value.clone());
        self.raw.entry(value).or_default().insert(interest);
    }

    pub fn find(&self, subject: &Subject) -> HashSet<&T> {
        let mut collector = HashSet::new();
        self.root
            .find_all_recursive(subject.segments(), &mut collector);
        collector
    }

    pub fn delete(&mut self, value: &T) {
        if let Some(interests) = self.raw.remove(value) {
            for interest in interests {
                let mut path = interest.as_segments();
                self.root.delete_recursive(&mut path, value);
            }
        }
    }

    pub fn interest_of(&self, value: &T) -> Option<&HashSet<Interest>> {
        self.raw.get(value)
    }
}

#[test]
fn test_interest_map() {
    let mut map = InterestMap::new();
    let interest = Interest::new("event/**/user/a");
    map.insert(interest, 1);
    map.insert(Interest::new("event/**/user/*"), 2);

    let values = map.find(&Subject::new("event/hello-world/user/a"));
    assert!(values.contains(&1));
    assert!(values.contains(&2));
}
