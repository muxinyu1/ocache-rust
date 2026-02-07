use std::{collections::HashMap, sync::Arc};

use rand::{
    Rng,
    distributions::{Alphanumeric, DistString},
    thread_rng,
};

use crate::kv::{byteview::ByteView, dbgetter::DbGetter, error::Error, peer::Peer};

mod kv;

fn generate_random_string(min: usize, max: usize) -> String {
    let mut rng = thread_rng();
    let length = rng.gen_range(min..=max);
    let string = Alphanumeric.sample_string(&mut rng, length);
    string
}

fn crc32_hasher(key: &str) -> usize {
    crc32fast::hash(key.as_bytes()) as usize
}

struct HashMapDbGetter {
    db: HashMap<String, Vec<(String, String)>>,
}

impl HashMapDbGetter {
    pub fn new(db: HashMap<String, Vec<(String, String)>>) -> Self {
        return HashMapDbGetter { db };
    }
}

impl DbGetter for HashMapDbGetter {
    fn get_data(&self, group_name: &str, key: &str) -> Result<ByteView, Error> {
        match self.db.get(group_name) {
            Some(table) => {
                for (k, v) in table {
                    if k == key {
                        return Ok(v.clone().into());
                    }
                }
                Err(format!("No such key: {}", key))
            }
            None => Err(format!("No such group: {}", group_name)),
        }
    }
}

fn main() {
    env_logger::init();
    log::info!("ocahce starting...");
    let mut db = HashMap::new();
    const GROUP_NAMES: [&str; 4] = ["Scores", "Tsinghua", "Labs", "Schools"];
    const KEYS: [&str; 4] = ["mxy", "oldust", "rust", "c++"];
    const MIN_LEN: usize = 8;
    const MAX_LEN: usize = 32;
    const NUM_THREAD: i32 = 2;
    const MAX_BYTES: usize = 256;
    const REPLICAS: i32 = 16;
    const BASE_PORT: i32 = 1024;
    // make db
    let mut group_infos = vec![];
    for group_name in GROUP_NAMES {
        if !db.contains_key(group_name) {
            db.insert(group_name.to_string(), vec![]);
        }
        let v = db.get_mut(group_name).unwrap();
        for key in KEYS {
            v.push((key.to_string(), generate_random_string(MIN_LEN, MAX_LEN)));
        }
        group_infos.push((group_name.to_string(), MAX_BYTES));
    }

    // print db
    println!("{:?}", db);

    // 打印 Key 分布情况
    println!("--- Key Distribution (Scores) ---");
    let mut ring = Vec::new();
    for i in 0..NUM_THREAD {
        let url = format!("http://127.0.0.1:{}", BASE_PORT + i);
        for r in 0..REPLICAS {
            let vnode = format!("{} {}", url, r);
            let h = crc32_hasher(&vnode);
            ring.push((h, url.clone()));
        }
    }
    ring.sort_by(|a, b| a.0.cmp(&b.0));

    for key in KEYS {
        let h = crc32_hasher(key);
        let idx = ring.partition_point(|x| x.0 < h);
        let peer = if idx >= ring.len() {
            &ring[0].1
        } else {
            &ring[idx].1
        };
        println!("Key: {:<10} -> {}", key, peer);
    }
    println!("---------------------------------");

    let getter = Arc::new(HashMapDbGetter::new(db));

    let mut handlers = vec![];
    for i in 0..NUM_THREAD {
        let getter_ptr = getter.clone();
        let group_infos = group_infos.clone();
        let handler = std::thread::spawn(move || {
            let peer = Peer::new(
                i,
                group_infos,
                getter_ptr,
                crc32_hasher,
                REPLICAS,
                BASE_PORT + i,
                NUM_THREAD,
            );
            peer.run();
        });
        handlers.push(handler);
    }
    for h in handlers {
        let _ = h.join();
    }
}
