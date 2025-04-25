use bytes::Bytes;

pub struct ClickhousePayload {
    pub rows: Vec<Bytes>
}

impl Default for ClickhousePayload {
    fn default() -> Self {
        Self {
            rows: Vec::new()
        }
    }
}