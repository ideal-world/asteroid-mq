//! # Interest
//! ## Match Interest
//! (/)?(<path>|<*>|<**>)/*
use std::collections::BTreeMap;

use bytes::Bytes;

/// # Interest
/// ## Glob Match Interest
/// (/)?(<path>|<*>|<**>)/*
#[derive(Debug, Clone)]
pub struct Interest(Bytes);
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
}

impl<T> Default for InterestMap<T> {
    fn default() -> Self {
        Self {
            root: Default::default(),
        }
    }
}

#[derive(Debug)]
pub struct InterestRadixTreeNode<T> {
    value: Option<T>,
    children: BTreeMap<Vec<u8>, InterestRadixTreeNode<T>>,
    any_child: Option<Box<InterestRadixTreeNode<T>>>,
    recursive_any_child: Option<Box<InterestRadixTreeNode<T>>>,
}
impl<T> Default for InterestRadixTreeNode<T> {
    fn default() -> Self {
        Self {
            value: None,
            children: BTreeMap::new(),
            any_child: None,
            recursive_any_child: None,
        }
    }
}

impl<T> InterestRadixTreeNode<T> {
    fn find_all_recursive<'a, 'i>(
        &'a self,
        mut path: impl Iterator<Item = InterestSegment<'i>> + Clone,
        collector: &mut Vec<&'a T>,
    ) {
        if let Some(next_seg) = path.next() {
            match next_seg {
                InterestSegment::Specific(seg) => {
                    if let Some(ref any) = self.any_child {
                        any.find_all_recursive(path.clone(), collector)
                    }
                    if let Some(ref r_any) = self.recursive_any_child {
                        r_any.find_all_recursive(path.clone(), collector)
                    }
                    if let Some(ref child) = self.children.get(seg.as_ref()) {
                        child.find_all_recursive(path, collector)
                    }
                }
                InterestSegment::Any => {
                    if let Some(ref any) = self.any_child {
                        any.find_all_recursive(path.clone(), collector)
                    }
                    if let Some(ref r_any) = self.recursive_any_child {
                        r_any.find_all_recursive(path, collector)
                    }
                }
                InterestSegment::RecursiveAny => {
                    if let Some(ref r_any) = self.recursive_any_child {
                        r_any.find_all_recursive(path, collector)
                    }
                }
            }
        } else if let Some(ref value) = self.value {
            collector.push(value)
        }
    }
}
impl<T> InterestMap<T> {
    pub fn new() -> Self {
        Self {
            root: InterestRadixTreeNode::default(),
        }
    }

    pub fn insert(&mut self, path: &[OwnedInterestSegment], value: T) {
        let mut current_node = &mut self.root;
        for segment in path {
            current_node = match segment {
                OwnedInterestSegment::Specific(s) => {
                    current_node.children.entry(s.clone()).or_default()
                }
                OwnedInterestSegment::Any => {
                    current_node.any_child.get_or_insert_with(Default::default)
                }
                OwnedInterestSegment::RecursiveAny => current_node
                    .recursive_any_child
                    .get_or_insert_with(Default::default),
            };
        }
        current_node.value = Some(value);
    }
    pub fn find_all(&self, interest: &Interest) -> Vec<T> {
        todo!("find all")
    }
}
