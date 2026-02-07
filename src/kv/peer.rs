use core::hash;
use std::{rc::Rc, sync::Arc};

use crate::kv::{
    byteview::ByteView,
    dbgetter::DbGetter,
    error::Error,
    groupmanager::GroupManager,
    httpclient::HttpClient,
    httpserver::HttpServer,
    peermanager::{Hasher, PeerSicker},
};

pub struct Peer {
    http_server: HttpServer,
    group_manager: GroupManager,
}

impl Peer {
    pub fn new(
        index: i32,
        group_infos: Vec<(String, usize)>,
        db_getter: Arc<dyn DbGetter>,
        hasher: Hasher,
        replicas: i32,
        port: i32,
        total_peers: i32,
    ) -> Peer {
        let group_manager = GroupManager::new(group_infos.clone(), db_getter);
        let mut peers = vec![];
        let base_port = port - index;
        for i in 0..total_peers {
            let base_url = format!("http://127.0.0.1:{}", base_port + i);
            if i == index {
                peers.push((base_url, None));
                continue;
            }
            let http_client = HttpClient::new(base_url.clone());
            let rc: Rc<dyn PeerClient> = Rc::new(http_client);
            peers.push((base_url, Some(rc)));
        }
        let peer_sicker = Arc::new(PeerSicker::new(hasher, replicas, &peers));
        for (group_name, _) in group_infos {
            if let Err(err) = group_manager.register_peer_for_group(peer_sicker.clone(), group_name)
            {
                panic!("{}", err);
            }
        }
        return Peer {
            http_server: HttpServer::new(port),
            group_manager,
        };
    }

    pub fn run(&self) {
        self.http_server.run(&self.group_manager);
    }
}

pub trait PeerClient {
    fn get_from_remote(&self, group: &str, key: &str) -> Result<ByteView, Error>;
    fn is_active(&self) -> bool;
}

pub trait PeerPicker {
    fn pick_peer(&self, key: &str) -> Option<Rc<dyn PeerClient>>;
}
