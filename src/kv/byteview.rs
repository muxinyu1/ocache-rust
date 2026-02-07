#[derive(Clone)]
pub struct ByteView {
    data: Vec<u8>,
}

impl ByteView {
    pub fn len(&self) -> usize {
        return self.data.len();
    }
    pub fn from_string(str: String) -> ByteView {
        return ByteView {
            data: str.into_bytes(),
        };
    }
}

impl Into<Vec<u8>> for ByteView {
    fn into(self) -> Vec<u8> {
        return self.data;
    }
}

impl From<String> for ByteView {
    fn from(value: String) -> Self {
        return ByteView { data: value.into() };
    }
}
