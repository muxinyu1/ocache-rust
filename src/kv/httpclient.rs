use ureq::{Agent, Error};

use crate::kv::{byteview::ByteView, peer::PeerClient};

pub struct HttpClient {
    base_url: String,
    client: Agent,
}

impl HttpClient {
    pub fn new(base_url: String) -> Self {
        return HttpClient {
            base_url,
            client: Agent::new(),
        };
    }
}

impl PeerClient for HttpClient {
    fn get_from_remote(
        &self,
        group: &str,
        key: &str,
    ) -> Result<super::byteview::ByteView, super::error::Error> {
        let url = format!("{}/{}/{}", self.base_url, group, key);
        log::info!("Fetching data from remote URL: {}", url);
        let resp = self.client.get(&url).call();
        match resp {
            Ok(resp) => {
                let status = resp.status();
                match resp.into_string() {
                    Ok(body) => {
                        if status != 200 {
                            Err(body)
                        } else {
                            Ok(ByteView::from_string(body))
                        }
                    }
                    Err(err) => Err(err.to_string()),
                }
            }
            Err(Error::Status(_code, resp)) => {
                if let Ok(msg) = resp.into_string() {
                    Err(msg)
                } else {
                    Err(format!("HTTP request failed: status code {}", _code))
                }
            }
            Err(err) => Err(err.to_string()),
        }
    }

    fn is_active(&self) -> bool {
        true
    }
}
