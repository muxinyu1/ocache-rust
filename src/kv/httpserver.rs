use tiny_http::{Response, Server};

use crate::kv::groupmanager::GroupManager;

pub struct HttpServer {
    // self_ip: String,
    // port: i32,
    server: Server
}

impl HttpServer {
    pub fn new(/*self_ip: String,*/ port: i32) -> Self {
        return HttpServer {
            // self_ip,
            // port,
            server: Server::http(format!("0.0.0.0:{}", port)).unwrap(),
        };
    }

    pub fn run(&self, group_manager: &GroupManager) {
        for req in self.server.incoming_requests() {
            let url = req.url();
            let parts: Vec<&str> = url.split("/").filter(|&s| !s.is_empty()).collect();
            if parts.len() != 2 {
                let _ = req.respond(Response::from_string("Not Found").with_status_code(404));
                continue;
            }
            let (group_name, key) = (parts[0], parts[1]);

            match group_manager.get(group_name, key) {
                Ok(view) => {
                    let _ = req.respond(Response::from_data(view).with_status_code(200));
                },
                Err(err) => {
                    let status = if err.contains("No such") { 404 } else { 500 };
                    let _ = req.respond(Response::from_string(err).with_status_code(status));
                },
            };
        }
    }
}
