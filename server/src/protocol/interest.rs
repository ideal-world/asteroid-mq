//! # Interest
//! ## Match Interest
//! (/)?(<path>|<*>|<**>)/*
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    hash::Hash,
};

pub use asteroid_mq_model::{
    Interest, InterestSegment, OwnedInterestSegment, Subject, SubjectSegments,
};
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone)]
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

#[derive(Clone)]
pub struct InterestRadixTreeNode<T> {
    value: HashSet<T>,
    children: BTreeMap<Vec<u8>, InterestRadixTreeNode<T>>,
    any_child: Option<Box<InterestRadixTreeNode<T>>>,
    recursive_any_child: Option<Box<InterestRadixTreeNode<T>>>,
}

struct ChildrenDebugProxy<'a, T>(&'a InterestRadixTreeNode<T>);

impl<T: std::fmt::Debug> std::fmt::Debug for ChildrenDebugProxy<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_map();
        debug.entries(
            self.0
                .children
                .iter()
                .map(|(k, v)| (std::str::from_utf8(k).unwrap_or("<invalid utf8 str>"), v)),
        );
        if let Some(any_child) = &self.0.any_child {
            debug.entry(&"*", any_child);
        }
        if let Some(recursive_any_child) = &self.0.recursive_any_child {
            debug.entry(&"**", recursive_any_child);
        }
        debug.finish()
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for InterestRadixTreeNode<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InterestRadixTreeNode")
            .field("value", &self.value)
            .field("children", &ChildrenDebugProxy(self))
            .finish()
    }
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

impl<T> Serialize for InterestMap<T>
where
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.raw.serialize(serializer)
    }
}

impl<'de, T> Deserialize<'de> for InterestMap<T>
where
    T: Deserialize<'de> + Hash + Eq + Clone,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = HashMap::<T, HashSet<Interest>>::deserialize(deserializer)?;
        Ok(Self::from_raw(raw))
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
