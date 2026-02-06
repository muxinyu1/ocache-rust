use std::sync::Arc;

use crate::kv::{
    byteview::ByteView, cache::Cache, dbgetter::DbGetter, error::Error, peer::PeerPicker,
};

pub struct Group {
    name: String,
    db_getter: Arc<dyn DbGetter>,
    peer_picker: Option<Arc<dyn PeerPicker>>,
    cache: Cache,
}

impl Group {
    pub fn new(name: String, db_getter: Arc<dyn DbGetter>, max_bytes: usize) -> Self {
        return Group {
            name: name,
            db_getter,
            peer_picker: None,
            cache: Cache::new(max_bytes),
        };
    }

    fn get_locally(&mut self, key: &str) -> Result<ByteView, Error> {
        match self.db_getter.get_data(&self.name, key) {
            Ok(view) => {
                self.cache.add(key, view.clone());
                Ok(view)
            }
            Err(err) => Err(err),
        }
    }
    pub fn register_peer(&mut self, picker: Arc<dyn PeerPicker>) -> Result<(), Error> {
        self.peer_picker = Some(picker);
        Ok(())
    }

    pub fn get(&mut self, key: &str) -> Result<ByteView, Error> {
        if key.is_empty() {
            return Err("key is empty".to_string());
        }
        log::info!("Group: {} searching for key: {}", self.name, key);
        // 缓存
        if let Some(res) = self.cache.get(key) {
            log::info!("Cache hit for key: {}", key);
            return Ok(res);
        }
        // peer
        if let Some(picker) = &self.peer_picker {
            if let Some(client) = picker.pick_peer(key) {
                log::info!("Pick peer for key: {}", key);
                return client.get_from_remote(&self.name, key);
            }
        }
        // 数据库
        log::info!("Database hit for key: {}", key);
        return self.get_locally(key);
    }
}
