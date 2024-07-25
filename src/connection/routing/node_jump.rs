use std::collections::HashMap;

use crate::connection::NodeId;
#[derive(Debug, Clone)]
pub struct NextJumpTable {
    pub table: HashMap<NodeId, NextJump>,
}

impl NextJumpTable {
    pub fn new() -> Self {
        Self {
            table: HashMap::new(),
        }
    }

    pub fn insert(&mut self, id: NodeId, hops: u32) {
        self.table.insert(id, NextJump { id, hops });
    }

    pub fn get(&self, id: &NodeId) -> Option<&NextJump> {
        self.table.get(id)
    }

    pub fn remove(&mut self, id: &NodeId) {
        self.table.remove(id);
    }

    pub fn clear(&mut self) {
        self.table.clear();
    }
}

#[derive(Debug, Clone)]
pub struct NextJump {
    pub id: NodeId,
    pub hops: u32,
}


