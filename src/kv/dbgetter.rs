use crate::kv::{byteview::ByteView, error::Error};

pub trait DbGetter {
    fn get_data(&self, group_name: &str, key: &str) -> Result<ByteView, Error>;
}