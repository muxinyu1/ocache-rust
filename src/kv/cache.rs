use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::kv::byteview::ByteView;

pub struct Cache {
    mtx: Arc<Mutex<LRUCache>>,
}

impl Cache {
    pub fn new(max_bytes: usize) -> Self {
        let lru = LRUCache::new(max_bytes);
        let mtx = Arc::new(Mutex::new(lru));
        return Cache { mtx };
    }
}

// 单线程LRU缓存
use std::ptr;

struct Node {
    key: String,
    value: ByteView,
    prev: *mut Node,
    next: *mut Node,
}

pub struct LRUCache {
    max_bytes: usize,
    current_bytes: usize,
    map: HashMap<String, *mut Node>,
    head: *mut Node, // 淘汰端 (Oldest)
    tail: *mut Node, // 活跃端 (Newest)
}

impl LRUCache {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            current_bytes: 0,
            map: HashMap::new(),
            head: ptr::null_mut(),
            tail: ptr::null_mut(),
        }
    }

    pub fn get(&mut self, key: &str) -> Option<ByteView> {
        let node_ptr = *self.map.get(key)?;
        unsafe {
            self.move_to_tail(node_ptr);
            Some((*node_ptr).value.clone())
        }
    }

    pub fn add(&mut self, key: &str, value: ByteView) {
        let new_len = value.len();
        if let Some(&node_ptr) = self.map.get(key) {
            unsafe {
                self.current_bytes = self.current_bytes - (*node_ptr).value.len() + new_len;
                (*node_ptr).value = value;
                self.move_to_tail(node_ptr);
            }
        } else {
            let node = Box::new(Node {
                key: key.to_string(),
                value,
                prev: ptr::null_mut(),
                next: ptr::null_mut(),
            });
            let node_ptr = Box::into_raw(node);
            self.map.insert(key.to_string(), node_ptr);
            self.link_to_tail(node_ptr);
            self.current_bytes += new_len;
        }

        while self.max_bytes > 0 && self.current_bytes > self.max_bytes {
            self.delete_by_lru();
        }
    }

    fn link_to_tail(&mut self, node: *mut Node) {
        unsafe {
            (*node).prev = self.tail;
            (*node).next = ptr::null_mut();
            if !self.tail.is_null() {
                (*self.tail).next = node;
            } else {
                self.head = node;
            }
            self.tail = node;
        }
    }

    fn move_to_tail(&mut self, node: *mut Node) {
        unsafe {
            if node == self.tail {
                return;
            }
            // 断开当前位置
            let p = (*node).prev;
            let n = (*node).next;
            if !p.is_null() {
                (*p).next = n;
            } else {
                self.head = n;
            }
            if !n.is_null() {
                (*n).prev = p;
            }
            // 挂到尾部
            self.link_to_tail(node);
        }
    }

    fn delete_by_lru(&mut self) {
        if self.head.is_null() {
            return;
        }
        unsafe {
            let old_node = self.head;
            self.head = (*old_node).next;
            if !self.head.is_null() {
                (*self.head).prev = ptr::null_mut();
            } else {
                self.tail = ptr::null_mut();
            }

            let node = Box::from_raw(old_node); // 转回 Box 触发自动释放
            self.map.remove(&node.key);
            self.current_bytes -= node.value.len();
        }
    }
}

impl Drop for LRUCache {
    fn drop(&mut self) {
        while !self.head.is_null() {
            self.delete_by_lru();
        }
    }
}

impl Cache {
    pub fn get(&self, key: &str) -> Option<ByteView> {
        let mut cache = self.mtx.lock().unwrap();
        let res = cache.get(key);
        if res.is_some() {
            log::debug!("LRU cache hit for key: {}", key);
        } else {
            log::debug!("LRU cache miss for key: {}", key);
        }
        res
    }
    pub fn add(&mut self, key: &str, value: ByteView) {
        log::debug!("Adding key: {} to LRU cache", key);
        let mut cache = self.mtx.lock().unwrap();
        cache.add(key, value);
    }
}
