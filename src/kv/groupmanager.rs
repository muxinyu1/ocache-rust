use std::{collections::HashMap, sync::{Arc, Mutex}};

use crate::kv::{byteview::ByteView, dbgetter::DbGetter, error::Error, group::Group, peer::PeerPicker};

pub struct GroupManager {
    mtx: Arc<Mutex<HashMap<String, Group>>>
}

impl GroupManager {

    pub fn new(group_infos: Vec<(String, usize)>, db_getter: Arc<dyn DbGetter>) -> Self {
        let mut map = HashMap::new();
        for (group_name, max_bytes) in group_infos {
            map.insert(group_name.to_string(), Group::new(group_name, db_getter.clone(), max_bytes));
        }
        return GroupManager { mtx: Arc::new(Mutex::new(map)) };
    }

    pub fn get(&self, group_name: &str, key: &str) -> Result<ByteView, Error> {
        let mut map = self.mtx.lock().unwrap();
        if let Some(group) = map.get_mut(group_name) {
            return group.get(key);
        }
        return Err(format!("No such group: {}", group_name));
    }
    pub fn register_peer_for_group(&self, picker: Arc<dyn PeerPicker>, group_name: String) -> Result<(), Error> {
        let mut map = self.mtx.lock().unwrap();
        if let Some(group) = map.get_mut(&group_name) {
            return group.register_peer(picker);
        }
        Err(format!("No such group: {}", group_name))
    }
}