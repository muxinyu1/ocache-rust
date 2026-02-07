use std::{collections::HashMap, rc::Rc};

use crate::kv::peer::{PeerClient, PeerPicker};

pub type Hasher = fn(&str) -> usize;
pub struct PeerSicker {
    hasher: Hasher,
    nodes: Vec<usize>,
    map: HashMap<usize, Option<Rc<dyn PeerClient>>>,
}

impl PeerSicker {
    pub fn new(
        hasher: Hasher,
        replicas: i32,
        peers: &Vec<(String, Option<Rc<dyn PeerClient>>)>,
    ) -> Self {
        let mut map = HashMap::new();
        let mut nodes = vec![];
        for (base_url, client) in peers {
            for i in 0..replicas {
                let replica_str = format!("{} {}", base_url, i);
                let hashed = hasher(&replica_str);
                nodes.push(hashed);
                map.insert(hashed, client.clone());
            }
        }
        // 排序
        nodes.sort();
        return PeerSicker { hasher, nodes, map };
    }
}

impl PeerPicker for PeerSicker {
    fn pick_peer(&self, key: &str) -> Option<Rc<dyn PeerClient>> {
        let hashed = (self.hasher)(key);
        let mut idx = self.nodes.partition_point(|&x| x < hashed);
        if idx >= self.nodes.len() {
            idx = 0;
        }
        let node_value = self.nodes[idx];
        if let Some(client) = self.map.get(&node_value) {
            return client.clone();
        }
        None
    }
}
